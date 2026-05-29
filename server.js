require('dotenv').config({ override: true });
const express = require('express');
const multer = require('multer');
const cors = require('cors');
const cookieParser = require('cookie-parser');
const { put: blobPut } = require('@vercel/blob');
const { randomUUID: _cryptoRandomUUID, randomBytes } = require('crypto');
const randomUUID = _cryptoRandomUUID
  ? () => _cryptoRandomUUID()
  : () => {
      const b = randomBytes(16);
      b[6] = (b[6] & 0x0f) | 0x40;
      b[8] = (b[8] & 0x3f) | 0x80;
      return b.toString('hex').replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5');
    };
const { Inngest } = require('inngest');
const { serve } = require('inngest/express');

const app = express();
const PORT = process.env.PORT || 3000;
const GEMINI_MODEL = 'gemini-2.5-pro';
const GEMINI_FALLBACK_MODEL = 'gemini-2.5-pro';

// ── Inngest client ──
const inngest = new Inngest({ id: 'receiptflow' });

// ── Background job: process one receipt from upload_queue ──
const processReceiptJob = inngest.createFunction(
  { id: 'process-receipt', retries: 3 },
  { event: 'receipt/queued' },
  async ({ event }) => {
    const { queueId } = event.data;
    const sb = await getSupabaseAdmin();

    const { data: row } = await sb.from('upload_queue')
      .select('*').eq('id', queueId).eq('status', 'pending').single();

    if (!row) return { message: 'Item not found or already processed' };

    const { data: claimed } = await sb.from('upload_queue')
      .update({ status: 'processing' })
      .eq('id', queueId).eq('status', 'pending').select('id');

    if (!claimed || claimed.length === 0) return { message: 'Already claimed by another run' };

    return await processOneQueueRow(sb, row);
  }
);

app.use(cors({
  origin: true,
  credentials: true
}));
app.use(express.json());
app.use(cookieParser((process.env.SESSION_SECRET || 'fallback-secret-change-me').trim()));

// ── Supabase admin client (lazy) ──
let supabaseAdmin = null;
async function getSupabaseAdmin() {
  if (supabaseAdmin) return supabaseAdmin;
  const { createClient } = await import('@supabase/supabase-js');
  supabaseAdmin = createClient(
    (process.env.SUPABASE_URL || '').trim(),
    (process.env.SUPABASE_SERVICE_KEY || '').trim(),
    { auth: { autoRefreshToken: false, persistSession: false } }
  );
  return supabaseAdmin;
}

// ── Public config endpoint (anon key safe to expose) ──
app.get('/api/config', (req, res) => {
  res.json({
    supabaseUrl: (process.env.SUPABASE_URL || '').trim(),
    supabaseAnonKey: (process.env.SUPABASE_ANON_KEY || '').trim()
  });
});

// ── Automated incoming receipt processor — registered BEFORE auth middleware ──
const getMimeType = (...names) => {
  for (const name of names) {
    const n = (name || '').toLowerCase();
    if (n.endsWith('.pdf')) return 'application/pdf';
    if (n.endsWith('.png')) return 'image/png';
    if (n.endsWith('.webp')) return 'image/webp';
    if (n.endsWith('.gif')) return 'image/gif';
    if (n.match(/\.jpe?g$/)) return 'image/jpeg';
  }
  return 'application/pdf';
};

const extractNodes = (jobsObj) => {
  if (!jobsObj) return [];
  if (Array.isArray(jobsObj.nodes) && jobsObj.nodes.length > 0) return jobsObj.nodes;
  if (Array.isArray(jobsObj.edges) && jobsObj.edges.length > 0) return jobsObj.edges.map(e => e.node).filter(Boolean);
  if (Array.isArray(jobsObj.nodes)) return jobsObj.nodes;
  return [];
};

// Validate that the extracted fields look like a real invoice/receipt.
// Filters out junk emails / random PDFs forwarded to the Gmail inbox,
// and bad scans where Gemini couldn't read meaningful content.
// Returns null if valid, otherwise a human-readable error string.
function validateReceiptFields(fields) {
  const reasons = [];

  const hasVendor = !!(fields.vendor && String(fields.vendor).trim().length >= 2);
  if (!hasVendor) reasons.push('vendor name');

  const totalNum = parseFloat(fields.total);
  const hasTotal = Number.isFinite(totalNum) && totalNum > 0;
  const hasItems = Array.isArray(fields.items) && fields.items.some(
    it => parseFloat(it.total) > 0 || parseFloat(it.unit) > 0
  );
  if (!hasTotal && !hasItems) reasons.push('total amount or line items');

  const hasIdentifier = !!(
    (fields.poNumber       && String(fields.poNumber).trim()) ||
    (fields.invoiceNo      && String(fields.invoiceNo).trim()) ||
    (fields.vendorInvoiceNo && String(fields.vendorInvoiceNo).trim())
  );
  if (!hasIdentifier) reasons.push('PO / invoice number');

  if (reasons.length === 0) return null;
  return `Document does not appear to be a valid invoice or receipt — missing ${reasons.join(', ')}.`;
}

// Detect if any value on the document is negative (credit note / return).
// ST's PO API doesn't accept negatives, so when we post a credit note we
// convert all amounts to absolute values and tag the description/memo so
// it's still identifiable in ST.
function isCreditNote({ total, tax, lineItems }) {
  const totalNum = parseFloat(total);
  const taxNum   = parseFloat(tax);
  if (Number.isFinite(totalNum) && totalNum < 0) return true;
  if (Number.isFinite(taxNum)   && taxNum   < 0) return true;
  if (Array.isArray(lineItems) && lineItems.some(
    it => parseFloat(it.total) < 0 || parseFloat(it.unit) < 0 || parseFloat(it.cost) < 0
  )) return true;
  return false;
}

// Stricter check at ServiceTitan-post time: refuses to push to ST when essential
// linking info is missing. Job number must be on the document (not derived).
function validateForServiceTitan({ vendor, jobId, lineItems, total }) {
  const missing = [];
  if (!vendor || !String(vendor).trim()) missing.push('vendor');
  if (!jobId  || !String(jobId).trim())  missing.push('job number');
  const hasNonzeroItem = Array.isArray(lineItems) && lineItems.some(
    it => Math.abs(parseFloat(it.total) || 0) > 0
       || Math.abs(parseFloat(it.unit)  || 0) > 0
       || Math.abs(parseFloat(it.cost)  || 0) > 0
  );
  const hasNonzeroTotal = Math.abs(parseFloat(total) || 0) > 0;
  if (!hasNonzeroItem && !hasNonzeroTotal) missing.push('line items / total');
  if (missing.length === 0) return null;
  return `Cannot post to ServiceTitan — missing ${missing.join(', ')}.`;
}

async function processRowCore(sb, incoming, fileBuffer, mimeType) {
  const rowId = incoming.id;
  const markFailed = async (errorMsg) => {
    console.error(`[process-incoming] ${rowId}: failed — ${errorMsg}`);
    await sb.from('incoming_receipts').update({
      status: 'failed', error: errorMsg, processed_at: new Date().toISOString()
    }).eq('id', rowId);
    return { id: rowId, success: false, error: errorMsg };
  };

  try {
    console.log(`[process-incoming] ${rowId}: processing ${fileBuffer.length} bytes, mime=${mimeType}`);

    const geminiOutput = await parseWithGeminiFallback(fileBuffer, mimeType);
    const fields = extractFieldsFromLlama(geminiOutput);
    console.log(`[process-incoming] ${rowId}: fields=`, JSON.stringify(fields));

    if (!fields.jobNo) return markFailed('No job number found on receipt');


    let jobberExpenseId = null;
    let jobberError = null;

    // Upload to Vercel Blob for a guaranteed public URL Jobber can fetch
    let receiptBlobUrl = null;
    if (process.env.BLOB_READ_WRITE_TOKEN) {
      try {
        const ext = mimeType === 'application/pdf' ? 'pdf' : (mimeType.split('/')[1] || 'jpg');
        const blobResult = await blobPut(`receipts/incoming_${incoming.id}.${ext}`, fileBuffer, {
          access: 'public',
          contentType: mimeType,
          token: process.env.BLOB_READ_WRITE_TOKEN
        });
        receiptBlobUrl = blobResult.url;
        console.log(`[process-incoming] ${rowId}: uploaded to Vercel Blob: ${receiptBlobUrl}`);
      } catch (blobErr) {
        console.error(`[process-incoming] ${rowId}: Blob upload failed, falling back to file_url:`, blobErr.message);
        receiptBlobUrl = incoming.file_url ||
          (incoming.storage_path ? `${process.env.SUPABASE_URL}/storage/v1/object/public/receipts/${incoming.storage_path}` : null);
      }
    } else {
      receiptBlobUrl = incoming.file_url ||
        (incoming.storage_path ? `${process.env.SUPABASE_URL}/storage/v1/object/public/receipts/${incoming.storage_path}` : null);
    }

    try {
      const numStr = String(parseInt(fields.jobNo, 10));
      let job = null;

      for (const term of [numStr, `#${numStr}`]) {
        const result = await jobberGQL(`
          query FindJob($term: String!) {
            jobs(first: 100, searchTerm: $term) {
              nodes { id jobNumber title }
              edges { node { id jobNumber title } }
            }
          }
        `, { term });
        if (!result.errors?.length) {
          job = extractNodes(result.data?.jobs).find(j => String(j.jobNumber) === numStr);
          if (job) break;
        }
      }

      if (!job) {
        let cursor = null;
        for (let page = 0; page < 20 && !job; page++) {
          const query = cursor
            ? `query PageJobs($cursor: String!) { jobs(first: 100, after: $cursor) { nodes { id jobNumber title } edges { node { id jobNumber title } } pageInfo { hasNextPage endCursor } } }`
            : `query PageJobs { jobs(first: 100) { nodes { id jobNumber title } edges { node { id jobNumber title } } pageInfo { hasNextPage endCursor } } }`;
          const result = await jobberGQL(query, cursor ? { cursor } : {});
          if (result.errors?.length) break;
          const jobsObj = result.data?.jobs;
          job = extractNodes(jobsObj).find(j => String(j.jobNumber) === numStr);
          if (job || !jobsObj?.pageInfo?.hasNextPage) break;
          cursor = jobsObj.pageInfo.endCursor;
        }
      }

      if (job) {
        const titleParts = [fields.vendor, fields.invoiceNo ? `Invoice #${fields.invoiceNo}` : null].filter(Boolean);
        const expInput = {
          linkedJobId: job.id,
          title: titleParts.length ? titleParts.join(' — ') : 'Expense',
          total: parseFloat(fields.total) || 0,
          date: (fields.date || new Date().toISOString().split('T')[0]) + 'T00:00:00Z'
        };
        if (fields.invoiceNo) expInput.description = `Invoice #${fields.invoiceNo}`;
        if (receiptBlobUrl) expInput.receiptUrl = receiptBlobUrl;

        const expResult = await jobberGQL(`
          mutation CreateExpense($input: ExpenseCreateInput!) {
            expenseCreate(input: $input) {
              expense { id title total }
              userErrors { message path }
            }
          }
        `, { input: expInput });

        const expense = expResult.data?.expenseCreate?.expense;
        const userErrors = expResult.data?.expenseCreate?.userErrors;
        if (userErrors?.length) console.warn(`[process-incoming] ${rowId}: Jobber userErrors:`, JSON.stringify(userErrors));
        console.log(`[process-incoming] ${rowId}: receiptBlobUrl sent=`, receiptBlobUrl);
        if (expense?.id) {
          jobberExpenseId = expense.id;
          console.log(`[process-incoming] ${rowId}: Jobber expense created: ${jobberExpenseId}`);
        } else {
          jobberError = userErrors?.[0]?.message || 'Expense creation returned no ID';
        }
      } else {
        jobberError = `Job #${fields.jobNo} not found in Jobber`;
      }
    } catch (jErr) {
      jobberError = jErr.message;
      console.error(`[process-incoming] ${rowId}: Jobber error:`, jErr.message);
    }

    if (!jobberExpenseId) return markFailed(jobberError || 'Jobber post failed');

    const receiptId = randomUUID();
    const { error: insertErr } = await sb.from('receipts').insert({
      id: receiptId,
      user_id: incoming.user_id,
      vendor: fields.vendor || null,
      date: fields.date || null,
      amount: parseFloat(fields.total) || 0,
      total: fields.total != null ? String(fields.total) : null,
      job_no: fields.jobNo || null,
      invoice_no: fields.invoiceNo || null,
      category: null,
      items: fields.items || [],
      receipt_blob_url: receiptBlobUrl,
      jobber_expense_id: jobberExpenseId,
      status: 'posted',
      error: null,
      saved_at: new Date().toISOString()
    });

    if (insertErr) {
      console.error(`[process-incoming] ${rowId}: receipts insert failed:`, insertErr.message);
      return markFailed(`DB insert failed: ${insertErr.message}`);
    }

    await sb.from('incoming_receipts').update({
      status: 'done', error: null, processed_at: new Date().toISOString()
    }).eq('id', rowId);

    console.log(`[process-incoming] ${rowId}: complete. receiptId=${receiptId}, jobberExpenseId=${jobberExpenseId}`);
    return { id: rowId, success: true, receiptId, jobberExpenseId };

  } catch (err) {
    console.error(`[process-incoming] ${rowId}: unhandled error:`, err);
    return markFailed(err.message || 'Unknown error');
  }
}

async function processOneRow(sb, incoming) {
  const rowId = incoming.id;
  const markFailed = async (errorMsg) => {
    console.error(`[process-incoming] ${rowId}: failed — ${errorMsg}`);
    await sb.from('incoming_receipts').update({
      status: 'failed', error: errorMsg, processed_at: new Date().toISOString()
    }).eq('id', rowId);
    return { id: rowId, success: false, error: errorMsg };
  };

  try {
    let fileBuffer, mimeType;
    if (incoming.file_url) {
      const fileRes = await fetch(incoming.file_url);
      if (!fileRes.ok) return markFailed(`Could not download file: HTTP ${fileRes.status}`);
      fileBuffer = Buffer.from(await fileRes.arrayBuffer());
      mimeType = getMimeType(incoming.file_name, incoming.storage_path, incoming.file_url);
    } else if (incoming.storage_path) {
      const { data: dlData, error: dlErr } = await sb.storage.from('receipts').download(incoming.storage_path);
      if (dlErr) return markFailed(`Storage download failed: ${dlErr.message}`);
      fileBuffer = Buffer.from(await dlData.arrayBuffer());
      mimeType = getMimeType(incoming.file_name, incoming.storage_path);
    } else {
      return markFailed('No file_url or storage_path on incoming record');
    }
    return processRowCore(sb, incoming, fileBuffer, mimeType);
  } catch (err) {
    return markFailed(err.message || 'Unknown error');
  }
}

// ── n8n endpoint: POST { rowId } → process a specific upload_queue row (Gmail receipts) ──
app.post('/api/process-queue-row', async (req, res) => {
  const secret = (process.env.PROCESS_SECRET || '').trim();
  const provided = (req.headers['x-process-secret'] || '').trim();
  if (secret && provided !== secret) return res.status(401).json({ error: 'Unauthorized' });

  const rowId = (req.body || {}).rowId;
  if (!rowId) return res.status(400).json({ error: 'Missing rowId' });

  const sb = await getSupabaseAdmin();
  const { data: row } = await sb.from('upload_queue').select('*').eq('id', rowId).single();
  if (!row) return res.status(404).json({ error: 'Row not found' });
  if (row.status !== 'pending') return res.json({ success: true, message: `Row already ${row.status}` });

  const { data: claimed } = await sb.from('upload_queue')
    .update({ status: 'processing' })
    .eq('id', rowId).eq('status', 'pending').select('id');

  if (!claimed || claimed.length === 0) return res.json({ success: true, message: 'Already claimed' });

  const result = await processOneQueueRow(sb, row);
  return res.json(result);
});

// ── n8n endpoint: POST { rowId } → Vercel downloads file directly from Supabase ──
app.post('/api/process-incoming-row', async (req, res) => {
  const secret = (process.env.PROCESS_SECRET || '').trim();
  const provided = (req.headers['x-process-secret'] || '').trim();
  if (secret && provided !== secret) return res.status(401).json({ error: 'Unauthorized' });

  const rowId = (req.body || {}).rowId;
  if (!rowId) return res.status(400).json({ error: 'Missing rowId' });

  const sb = await getSupabaseAdmin();
  const { data: row } = await sb.from('upload_queue').select('*').eq('id', rowId).single();
  if (!row) return res.status(404).json({ error: 'Row not found' });
  if (row.status !== 'pending') return res.json({ success: true, message: `Row already ${row.status}` });

  const { data: claimed } = await sb.from('upload_queue')
    .update({ status: 'processing' })
    .eq('id', rowId).eq('status', 'pending').select('id');

  if (!claimed || claimed.length === 0) return res.json({ success: true, message: 'Already claimed' });

  const result = await processOneQueueRow(sb, row);
  return res.json(result);
});

// ── Process a manually-uploaded receipt for ServiceTitan (background) ──
async function processIncomingForST(sb, incoming) {
  const rowId = incoming.id;
  const markFailed = async (errorMsg) => {
    console.error(`[process-st] ${rowId}: failed — ${errorMsg}`);
    await sb.from('incoming_receipts').update({
      status: 'failed', error: errorMsg, processed_at: new Date().toISOString()
    }).eq('id', rowId);
  };

  try {
    let fileBuffer, mimeType;
    mimeType = getMimeType(incoming.file_name, incoming.storage_path, incoming.file_url);

    if (incoming.storage_path) {
      // Download directly via service role — works regardless of bucket policy
      const { data: fileData, error: dlErr } = await sb.storage
        .from('receipts')
        .download(incoming.storage_path);
      if (dlErr) { await markFailed('Storage download failed: ' + dlErr.message); return; }
      fileBuffer = Buffer.from(await fileData.arrayBuffer());
    } else {
      // Fallback: public URL fetch
      const fileRes = await fetch(incoming.file_url);
      if (!fileRes.ok) { await markFailed(`Could not download file: HTTP ${fileRes.status}`); return; }
      fileBuffer = Buffer.from(await fileRes.arrayBuffer());
    }

    console.log(`[process-st] ${rowId}: ${fileBuffer.length} bytes, mime=${mimeType}`);

    await sb.from('incoming_receipts').update({ status: 'processing' }).eq('id', rowId);

    const geminiOutput = await parseWithGeminiFallback(fileBuffer, mimeType);
    const fields = extractFieldsFromLlama(geminiOutput);
    console.log(`[process-st] ${rowId}: fields=`, JSON.stringify(fields));

    const validationError = validateReceiptFields(fields);
    if (validationError) {
      console.warn(`[process-st] ${rowId}: rejected — ${validationError}`);
      await markFailed(validationError);
      return;
    }

    // File is already in Supabase Storage — use its public URL directly
    const receiptBlobUrl = incoming.file_url;

    const receiptId = randomUUID();
    const { error: insertErr } = await sb.from('receipts').insert({
      id: receiptId,
      user_id: incoming.user_id,
      vendor: fields.vendor || null,
      date: fields.date || null,
      amount: parseFloat(fields.total) || 0,
      total: fields.total != null ? String(fields.total) : null,
      tax: fields.tax != null ? fields.tax : null,
      shipping: fields.shipping != null ? fields.shipping : null,
      job_no: fields.jobNo || null,
      invoice_no: fields.invoiceNo || null,
      po_number: fields.poNumber || null,
      required_date: fields.requiredDate || null,
      vendor_invoice_no: fields.vendorInvoiceNo || null,
      category: null,
      items: fields.items || [],
      receipt_blob_url: receiptBlobUrl,
      status: 'pending',
      error: null,
      saved_at: new Date().toISOString()
    });

    if (insertErr) {
      console.error(`[process-st] ${rowId}: receipts insert error (continuing):`, insertErr.message);
    }

    const resultData = {
      vendor: fields.vendor || null,
      invoiceNo: fields.invoiceNo || null,
      date: fields.date || null,
      total: fields.total || null,
      tax: fields.tax != null ? fields.tax : null,
      shipping: fields.shipping != null ? fields.shipping : null,
      jobNo: fields.jobNo || null,
      poNumber: fields.poNumber || null,
      requiredDate: fields.requiredDate || null,
      vendorInvoiceNo: fields.vendorInvoiceNo || null,
      items: fields.items || [],
      receiptBlobUrl,
      receiptId: insertErr ? null : receiptId
    };

    await sb.from('incoming_receipts').update({
      status: 'done', error: null, processed_at: new Date().toISOString(),
      result: resultData
    }).eq('id', rowId);

    console.log(`[process-st] ${rowId}: complete. receiptId=${receiptId}`);
  } catch (err) {
    console.error(`[process-st] ${rowId}: unhandled error:`, err);
    await markFailed(err.message || 'Unknown error');
  }
}

// ── Process manual upload queue (queue-based, no auto-post) ──
async function processOneQueueRow(sb, row) {
  const rowId = row.id;
  const markFailed = async (errorMsg) => {
    console.error(`[process-queue] ${rowId}: failed — ${errorMsg}`);
    await sb.from('upload_queue').update({
      status: 'failed', error: errorMsg, processed_at: new Date().toISOString()
    }).eq('id', rowId);
    return { id: rowId, success: false, error: errorMsg };
  };

  try {
    const fileRes = await fetch(row.file_url);
    if (!fileRes.ok) return markFailed(`Could not download file: HTTP ${fileRes.status}`);
    const fileBuffer = Buffer.from(await fileRes.arrayBuffer());
    const mimeType = getMimeType(row.file_name);

    console.log(`[process-queue] ${rowId}: downloaded ${fileBuffer.length} bytes, mime=${mimeType}`);

    const geminiOutput = await parseWithGeminiFallback(fileBuffer, mimeType);
    const fields = extractFieldsFromLlama(geminiOutput);
    console.log(`[process-queue] ${rowId}: fields=`, JSON.stringify(fields));

    const validationError = validateReceiptFields(fields);
    if (validationError) {
      console.warn(`[process-queue] ${rowId}: rejected as junk — ${validationError}`);
      return markFailed(validationError);
    }

    const now = new Date().toISOString();
    await sb.from('upload_queue').update({
      status: 'done',
      vendor: fields.vendor || null,
      invoice_no: fields.invoiceNo || null,
      date: fields.date || null,
      amount: parseFloat(fields.total) || 0,
      job_no: fields.jobNo || null,
      items: fields.items || [],
      error: null,
      processed_at: now
    }).eq('id', rowId);

    // Insert into receipts table so it appears in history
    const receiptId = randomUUID();
    const { error: insertErr } = await sb.from('receipts').insert({
      id:               receiptId,
      user_id:          row.user_id || null,
      file_name:        row.file_name || null,
      vendor:           fields.vendor || null,
      date:             fields.date || null,
      amount:           parseFloat(fields.total) || 0,
      total:            fields.total != null ? String(fields.total) : null,
      tax:              fields.tax != null ? fields.tax : null,
      shipping:         fields.shipping != null ? fields.shipping : null,
      job_no:           fields.jobNo || null,
      invoice_no:       fields.invoiceNo || null,
      po_number:        fields.poNumber || null,
      required_date:    fields.requiredDate || null,
      vendor_invoice_no: fields.vendorInvoiceNo || null,
      category:         null,
      items:            fields.items || [],
      receipt_blob_url: row.file_url || null,
      status:           'saved',
      source:           'gmail',
      error:            null,
      saved_at:         now
    });
    if (insertErr) {
      console.error(`[process-queue] ${rowId}: receipts insert error:`, insertErr.message);
    }

    // Auto-post to ServiceTitan as a Purchase Order
    let stPoId = null;
    try {
      const stBlocker = validateForServiceTitan({
        vendor:    fields.vendor,
        jobId:     fields.jobNo,
        lineItems: fields.items,
        total:     fields.total,
        tax:       fields.tax
      });
      if (stBlocker) {
        console.warn(`[process-queue] ${rowId}: skipping ST post — ${stBlocker}`);
        throw new Error(stBlocker);
      }
      const stResult = await createSTPurchaseOrder({
        poNumber:        fields.poNumber         || null,
        vendor:          fields.vendor           || null,
        vendorInvoiceNo: fields.vendorInvoiceNo  || fields.invoiceNo || null,
        date:            fields.date             || null,
        requiredDate:    fields.requiredDate     || null,
        tax:             fields.tax              ?? null,
        shipping:        fields.shipping         ?? null,
        jobId:           fields.jobNo            || null,
        lineItems:       fields.items            || [],
        total:           fields.total            ?? null
      });
      stPoId = stResult.poId;
      console.log(`[process-queue] ${rowId}: ST PO created → id=${stPoId}`);
      // Mark receipt as pushed in both tables
      if (!insertErr) {
        await sb.from('receipts').update({ status: 'pushed', st_purchase_order_id: String(stPoId) }).eq('id', receiptId);
      }
      await sb.from('upload_queue').update({ st_purchase_order_id: String(stPoId) }).eq('id', rowId).catch(e => {
        console.warn(`[process-queue] ${rowId}: could not write st_purchase_order_id to upload_queue:`, e.message);
      });
    } catch (stErr) {
      console.error(`[process-queue] ${rowId}: ST post failed:`, stErr.message);
    }

    return { id: rowId, success: true, stPoId };
  } catch (err) {
    console.error(`[process-queue] ${rowId}: unhandled error:`, err);
    return markFailed(err.message || 'Unknown error');
  }
}

app.all('/api/process-queue', async (req, res) => {
  const secret = (process.env.PROCESS_SECRET || '').trim();
  const cronSecret = (process.env.CRON_SECRET || '').trim();
  const provided = (req.headers['x-process-secret'] || '').trim();
  const bearer = (req.headers.authorization || '').replace(/^Bearer\s+/i, '').trim();

  const validSecret = secret && provided === secret;
  const validCron = cronSecret && bearer === cronSecret;
  const noAuthConfigured = !secret && !cronSecret;

  if (!noAuthConfigured && !validSecret && !validCron) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const sb = await getSupabaseAdmin();

  // Reset stuck 'processing' rows back to pending
  await sb.from('upload_queue')
    .update({ status: 'pending', error: null })
    .eq('status', 'processing');

  // Only fetch ONE item — Vercel Hobby plan caps function runtime at 60s.
  // GitHub Actions loops this endpoint until no pending items remain.
  const { data: pending, error: selectErr } = await sb
    .from('upload_queue')
    .select('*')
    .eq('status', 'pending')
    .order('created_at', { ascending: true })
    .limit(1);

  if (selectErr) return res.status(500).json({ error: selectErr.message });
  if (!pending || pending.length === 0) return res.json({ success: true, pending: 0, message: 'No pending items' });

  const row = pending[0];
  console.log(`[process-queue] processing 1 item: ${row.id}`);

  const { data: claimed } = await sb
    .from('upload_queue')
    .update({ status: 'processing' })
    .eq('id', row.id)
    .eq('status', 'pending')
    .select('id');

  if (!claimed || claimed.length === 0) {
    return res.json({ success: true, pending: 0, message: 'Item already claimed' });
  }

  const result = await processOneQueueRow(sb, row);

  // Check how many are still pending so the caller knows whether to loop
  const { count } = await sb.from('upload_queue').select('id', { count: 'exact', head: true }).eq('status', 'pending');
  return res.json({ success: true, pending: count || 0, result });
});

app.all('/api/process-incoming', async (req, res) => {
  // Accept either x-process-secret (n8n/GitHub Actions) or Vercel's auto cron Bearer token
  const secret = (process.env.PROCESS_SECRET || '').trim();
  const cronSecret = (process.env.CRON_SECRET || '').trim();
  const provided = (req.headers['x-process-secret'] || '').trim();
  const bearer = (req.headers.authorization || '').replace(/^Bearer\s+/i, '').trim();

  const validSecret = secret && provided === secret;
  const validCron = cronSecret && bearer === cronSecret;
  const noAuthConfigured = !secret && !cronSecret;

  if (!noAuthConfigured && !validSecret && !validCron) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const sb = await getSupabaseAdmin();

  // Reset ALL rows stuck in 'processing' — if processing completed they'd be 'done'/'failed'
  await sb.from('incoming_receipts')
    .update({ status: 'pending', error: null })
    .eq('status', 'processing');

  // Fetch ALL pending rows
  const { data: allPending, error: selectErr } = await sb
    .from('incoming_receipts')
    .select('*')
    .eq('status', 'pending')
    .order('created_at', { ascending: true });

  if (selectErr) return res.status(500).json({ error: selectErr.message });
  if (!allPending || allPending.length === 0) return res.json({ success: true, message: 'No pending receipts' });

  // Deduplicate by file_url — mark extras as done so they never re-queue
  const seenUrls = new Set();
  const toProcess = [], dupes = [];
  for (const r of allPending) {
    if (seenUrls.has(r.file_url)) dupes.push(r);
    else { seenUrls.add(r.file_url); toProcess.push(r); }
  }
  for (const d of dupes) {
    await sb.from('incoming_receipts').update({
      status: 'done', error: 'Duplicate file — skipped', processed_at: new Date().toISOString()
    }).eq('id', d.id);
  }

  if (toProcess.length === 0) return res.json({ success: true, message: 'No pending receipts after dedup', skipped: dupes.length });

  console.log(`[process-incoming] ${toProcess.length} unique file(s) to process, ${dupes.length} duplicate(s) skipped`);

  // Process ALL pending rows sequentially — one failure won't block the others
  const results = [];
  for (const pending of toProcess) {
    const { data: claimed } = await sb
      .from('incoming_receipts')
      .update({ status: 'processing' })
      .eq('id', pending.id)
      .eq('status', 'pending')
      .select('id');

    if (!claimed || claimed.length === 0) {
      console.log(`[process-incoming] ${pending.id}: already claimed, skipping`);
      results.push({ id: pending.id, skipped: true });
      continue;
    }

    const result = await processOneRow(sb, pending);
    results.push(result);
  }

  const posted = results.filter(r => r.success).length;
  const failed = results.filter(r => !r.success && !r.skipped).length;
  return res.json({ success: true, processed: toProcess.length, posted, failed, skipped: dupes.length, results });
});

// ── Inngest serve endpoint (must be before auth middleware) ──
app.use('/api/inngest', serve({
  client: inngest,
  functions: [processReceiptJob]
}));

// ── Auth middleware — verifies Supabase JWT ──
app.use(async (req, res, next) => {
  if (req.path === '/api/process-incoming' || req.path === '/api/process-queue' || req.path === '/api/process-incoming-row' || req.path === '/api/process-queue-row') return next();
  if (req.path.startsWith('/api/inngest')) return next();
  const open = [
    '/api/config',
    '/api/health',
    '/api/auth/jobber',
    '/api/auth/callback',
    '/api/jobber-status',
    '/api/jobber-debug',
    '/api/extract',
    '/api/extract-url',
    '/api/create-po',
    '/api/create-expense',
    '/api/queue-manual-upload',
    '/api/receipts-list',
    '/api/gmail-receipts',
    '/api/receipt-status',
    '/api/receipt/',
    '/api/incoming-history',
    '/api/incoming-pending',
    '/api/test-st',
    '/api/test-st-returns'
  ];

  if (!req.path.startsWith('/api/') || open.includes(req.path) || req.path.startsWith('/api/incoming-status/')) return next();

  const token = (req.headers.authorization || '').replace('Bearer ', '').trim();
  if (!token) return res.status(401).json({ error: 'Not authenticated', code: 'NOT_AUTHENTICATED' });

  try {
    const sb = await getSupabaseAdmin();
    const { data: { user }, error } = await sb.auth.getUser(token);
    if (error || !user) return res.status(401).json({ error: 'Not authenticated', code: 'NOT_AUTHENTICATED' });
    req.user = user;
    return next();
  } catch {
    return res.status(401).json({ error: 'Not authenticated', code: 'NOT_AUTHENTICATED' });
  }
});

const storage = multer.memoryStorage();
const upload = multer({
  storage,
  limits: { fileSize: 4 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const allowed = ['image/jpeg', 'image/png', 'image/webp', 'image/gif', 'application/pdf'];
    if (allowed.includes(file.mimetype)) cb(null, true);
    else cb(new multer.MulterError('LIMIT_UNEXPECTED_FILE', 'Only JPG, PNG, WEBP, GIF and PDF files are allowed'));
  }
});

function extractFieldsFromLlama(content) {
  let vendor = null, invoiceNo = null, date = null, jobNo = null, total = null;
  let poNumber = null, requiredDate = null, vendorInvoiceNo = null;
  const items = [];
  const isYear = (s) => /^20[12]\d$/.test(s);

  function parseTables(input) {
    const tbls = [];

    for (const [tableHtml] of input.matchAll(/<table[\s\S]*?<\/table>/gi)) {
      const rows = [];
      for (const [rowHtml] of tableHtml.matchAll(/<tr[\s\S]*?<\/tr>/gi)) {
        const cells = [];
        for (const [, inner] of rowHtml.matchAll(/<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi)) {
          const text = inner
            .replace(/<br\s*\/?>/gi, ' ')
            .replace(/<[^>]+>/g, '')
            .replace(/&nbsp;/g, ' ')
            .replace(/&#160;/g, ' ')
            .replace(/\u00A0/g, ' ')
            .replace(/&amp;/g, '&')
            .replace(/&lt;/g, '<')
            .replace(/&gt;/g, '>')
            .replace(/\s+/g, ' ')
            .trim();
          cells.push(text);
        }
        if (cells.some(c => c.length > 0)) rows.push(cells);
      }
      if (rows.length) tbls.push(rows);
    }

    const htmlZapped = input.replace(/<table[\s\S]*?<\/table>/gi, '');
    const pipeLines = htmlZapped.split('\n');
    let mdBlock = [];

    const flushMdBlock = () => {
      if (mdBlock.length >= 2) tbls.push(mdBlock);
      mdBlock = [];
    };

    for (const line of pipeLines) {
      const trimmed = line.trim();
      if (!trimmed.startsWith('|')) {
        flushMdBlock();
        continue;
      }
      if (/^\|[\s|:-]+\|$/.test(trimmed)) continue;

      const cells = trimmed
        .replace(/^\||\|$/g, '')
        .split('|')
        .map(c => c.trim())
        .filter((_, i, arr) => i < arr.length);

      if (cells.some(c => c.length > 0)) mdBlock.push(cells);
    }

    flushMdBlock();
    return tbls;
  }

  function parseDate(str) {
    if (!str) return null;
    if (/^\d{4}-\d{2}-\d{2}$/.test(str)) return str;

    // YYYY/MM/DD or YYYY-MM-DD
    let m = str.match(/(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})/);
    if (m) return `${m[1]}-${m[2].padStart(2, '0')}-${m[3].padStart(2, '0')}`;

    // DD/MM/YYYY (preferred for Canadian invoices) or MM/DD/YYYY — ambiguous,
    // pick DMY when first part > 12 (only DMY makes sense), else stay with the original MDY guess.
    m = str.match(/(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})/);
    if (m) {
      const a = parseInt(m[1], 10), b = parseInt(m[2], 10);
      if (a > 12) return `${m[3]}-${b.toString().padStart(2,'0')}-${a.toString().padStart(2,'0')}`;
      return `${m[3]}-${m[1].padStart(2, '0')}-${m[2].padStart(2, '0')}`;
    }

    // 2-digit year: DD/MM/YY (e.g. Master invoices print "11/05/26")
    m = str.match(/(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2})\b/);
    if (m) {
      const yy = parseInt(m[3], 10);
      const yyyy = yy < 70 ? 2000 + yy : 1900 + yy;
      const a = parseInt(m[1], 10), b = parseInt(m[2], 10);
      // Assume DMY for 2-digit-year invoices (common in CA/EU formats)
      if (a > 12) return `${yyyy}-${b.toString().padStart(2,'0')}-${a.toString().padStart(2,'0')}`;
      if (b > 12) return `${yyyy}-${m[1].padStart(2,'0')}-${m[2].padStart(2,'0')}`;
      return `${yyyy}-${m[2].padStart(2,'0')}-${m[1].padStart(2,'0')}`;
    }

    // Month-name format: "May 14, 2026" / "May 14 2026" / "14 May 2026"
    const months = { jan:1, feb:2, mar:3, apr:4, may:5, jun:6, jul:7, aug:8, sep:9, oct:10, nov:11, dec:12 };
    m = str.match(/([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{4})/);
    if (m) {
      const mo = months[m[1].slice(0,3).toLowerCase()];
      if (mo) return `${m[3]}-${String(mo).padStart(2,'0')}-${m[2].padStart(2,'0')}`;
    }
    m = str.match(/(\d{1,2})\s+([A-Za-z]{3,9}),?\s+(\d{4})/);
    if (m) {
      const mo = months[m[2].slice(0,3).toLowerCase()];
      if (mo) return `${m[3]}-${String(mo).padStart(2,'0')}-${m[1].padStart(2,'0')}`;
    }

    return null;
  }

  const tables = parseTables(content);
  const lmap = {};

  // Labels where we want the largest monetary value (grand total > line total)
  const MONETARY_LABELS = new Set([
    'TOTAL', 'GRAND TOTAL', 'INVOICE TOTAL', 'AMOUNT DUE',
    'BALANCE DUE', 'TOTAL DUE', 'SUBTOTAL', 'SUB-TOTAL', 'SUB TOTAL',
    'GROSS TOTAL', 'TOTAL GOODS'
  ]);

  const setLmap = (lbl, val) => {
    if (MONETARY_LABELS.has(lbl)) {
      const newN = parseFloat((val || '').replace(/[$,]/g, '').replace(/\s*[-+]\s*$/, '').replace(/^-/, ''));
      const oldN = parseFloat((lmap[lbl] || '').replace(/[$,]/g, '').replace(/\s*[-+]\s*$/, '').replace(/^-/, ''));
      // Compare absolute values — tax-inclusive total (807.91) > pre-tax gross (721.35)
      if (!isNaN(newN) && (isNaN(oldN) || newN > oldN)) {
        lmap[lbl] = val;
      }
    } else {
      lmap[lbl] = val;
    }
  };

  for (const table of tables) {
    for (let r = 0; r < table.length; r++) {
      const row = table[r];

      if (r + 1 < table.length) {
        const vRow = table[r + 1];
        const isLabelRow =
          row.some(c => /^[A-Z][A-Z\s./()#-]{2,}$/.test(c)) &&
          !row.some(c => /^\d{2}[\/\-]\d{2}[\/\-]\d{4}$/.test(c)) &&
          !row.some(c => /^\d+\.\d{2}$/.test(c));

        const isValueRow =
          vRow.some(c => c.length > 0) &&
          !vRow.every(c => /^[A-Z][A-Z\s./()#-]{2,}$/.test(c) || c === '');

        if (isLabelRow && isValueRow) {
          for (let c = 0; c < row.length; c++) {
            const lbl = row[c].toUpperCase().replace(/\s+/g, ' ').trim();
            const val = (vRow[c] || '').trim();
            if (lbl.length > 1 && val && !/^[A-Z][A-Z\s./()#-]{4,}$/.test(val)) {
              setLmap(lbl, val);
            }
          }
        }
      }

      const rowIsAllLabels = row.every(c => c === '' || /^[A-Z][A-Z\s./()#-]{2,}$/.test(c));
      if (!rowIsAllLabels) {
        for (let c = 0; c + 1 < row.length; c++) {
          const lbl = row[c].replace(/:$/, '').toUpperCase().replace(/\s+/g, ' ').trim();
          const val = row[c + 1].trim();
          if (
            /^[A-Z][A-Z\s./()#-]{2,}$/.test(lbl) &&
            val &&
            !/^[A-Z][A-Z\s./()#-]{4,}$/.test(val) &&
            val !== lbl
          ) {
            setLmap(lbl, val);
            c++;
          }
        }
      }
    }
  }

  const plainText = content.replace(/<table[\s\S]*?<\/table>/gi, '');
  for (const [, lbl, val] of plainText.matchAll(/^([A-Z][A-Za-z\s./()#-]{2,}?)\s*:\s*(.+)$/gm)) {
    const k = lbl.toUpperCase().replace(/\s+/g, ' ').trim();
    if (k && val.trim()) setLmap(k, val.trim());
  }

  console.log('[fields] label map:', JSON.stringify(lmap));

  const DOC_KEYWORDS = /^(invoice|receipt|packing\s*slip|counter\s*sale|return|original|copy|statement|order|quote|estimate|bill)/i;
  const headingMatch = [...content.matchAll(/^#{1,2}\s+([A-Za-z][A-Za-z0-9\s&.,'()-]+?)$/gm)]
    .map(m => m[1].trim())
    .find(h => !DOC_KEYWORDS.test(h));

  const boldMatch = content.match(/\*\*([A-Za-z][A-Za-z\s&.-]+?)\*\*/);

  // Explicit label lookups first — more reliable than heading detection
  vendor =
    lmap['VENDOR'] ||
    lmap['SUPPLIER'] ||
    lmap['SOLD BY'] ||
    lmap['BILLED BY'] ||
    lmap['BILL FROM'] ||
    lmap['INVOICE FROM'] ||
    lmap['COMPANY'] ||
    // Fall back to heading/bold only if no explicit label found
    headingMatch ||
    (boldMatch?.[1]?.trim() && !DOC_KEYWORDS.test(boldMatch[1]) ? boldMatch[1].trim() : null) ||
    null;

  // Normalized lookup helper — strips dots entirely (so "P.S.T." → "PST"),
  // converts slashes / ampersands to spaces (so "G.S.T./H.S.T." → "GST HST"),
  // and collapses extra whitespace.
  const normLabel = s => String(s || '').toUpperCase().replace(/\./g, '').replace(/[\/&]/g, ' ').replace(/\s+/g, ' ').trim();
  const lmapNorm = {};
  for (const [k, v] of Object.entries(lmap)) lmapNorm[normLabel(k)] = v;
  const getL = (...keys) => {
    for (const k of keys) {
      const exact = lmap[k];
      if (exact) return exact;
      const norm = lmapNorm[normLabel(k)];
      if (norm) return norm;
    }
    return null;
  };

  invoiceNo = getL(
    'INVOICE NO', 'INVOICE NUMBER', 'INVOICE #', 'INVOICE',
    'ORDER NO', 'ORDER NUMBER',
    'DOCUMENT NO', 'DOCUMENT NUMBER',
    'RECEIPT NO', 'RECEIPT NUMBER',
    'TRANSACTION NO', 'TRANSACTION NUMBER'
  );

  const rawDate =
    lmap['INVOICE DATE'] ||
    lmap['DATE'] ||
    lmap['TRANSACTION DATE'] ||
    lmap['BILL DATE'] ||
    lmap['SALE DATE'] ||
    lmap['RECEIPT DATE'] ||
    lmap['ISSUED'] ||
    lmap['ORDER DATE'] ||
    lmap['INFORMATION'] ||   // Andrew Sheret invoices print the date in the "Information" row
    null;

  // For 2-digit-year invoices like Master ("22/05/26"), Gemini sometimes
  // interprets the day-month-year as year-month-day and outputs the swapped
  // ISO date (e.g. 2022-05-26 instead of 2026-05-22). Detect implausible
  // years and try the swap; pick whichever lands closer to today.
  const fixSwappedYearDay = (iso) => {
    if (!iso) return iso;
    const m = String(iso).match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (!m) return iso;
    const year = parseInt(m[1], 10);
    const day  = parseInt(m[3], 10);
    const currentYear = new Date().getFullYear();
    if (year < currentYear - 2 || year > currentYear + 2) {
      const newYearLast2 = day;
      const newDay = year >= 2000 ? year - 2000 : year - 1900;
      const newYear = newYearLast2 < 70 ? 2000 + newYearLast2 : 1900 + newYearLast2;
      if (newDay >= 1 && newDay <= 31 &&
          Math.abs(newYear - currentYear) < Math.abs(year - currentYear)) {
        const swapped = `${newYear}-${m[2]}-${String(newDay).padStart(2,'0')}`;
        console.warn(`[fields] suspicious date ${iso} (year off by ${Math.abs(year - currentYear)}); swapping year↔day → ${swapped}`);
        return swapped;
      }
    }
    return iso;
  };

  date = parseDate(rawDate);
  if (!date) {
    // Numeric formats first (MM/DD/YYYY or DD/MM/YYYY)
    const anyDate = content.match(/\b(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{4})\b/);
    if (anyDate) date = parseDate(anyDate[1]);
  }
  if (!date) {
    // Month-name format: "May 14, 2026" — used by Andrew Sheret-style invoices
    // where the date sits in an "Information" row Gemini sometimes flattens to text.
    const anyMonth = content.match(/\b([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{4})\b/);
    if (anyMonth) date = parseDate(anyMonth[0]);
  }
  // Apply swap heuristic in case parseDate honored a flipped input from Gemini
  date = fixSwappedYearDay(date);

  const JOB_LABELS = [
    'YOUR P.O. NO', 'YOUR P.O.NO', 'P.O. NO', 'P.O.NO', 'PO NO', 'PO #',
    'PO NUMBER', 'PO NUMBER:', 'PURCHASE ORDER NUMBER',
    'PURCHASE ORDER', 'PURCHASE ORDER NO', 'CUSTOMER PO', 'CUST PO',
    'JOB NO', 'JOB #', 'JOB NUMBER', 'JOB ID',
    'WORK ORDER', 'WORK ORDER NO', 'WO #', 'W.O. NO',
    'YOUR REF', 'YOUR REFERENCE', 'CUSTOMER REF', 'REF NO',
  ];

  for (const lbl of JOB_LABELS) {
    const val = (lmap[lbl] || '').trim();
    if (!val) continue;

    let m = val.match(/^(\d{3,9})$/);
    if (m && !isYear(m[1])) {
      jobNo = m[1];
      break;
    }

    // Handle "1095 - RETURN", "1391-CREDIT" etc. with optional spaces around dash
    m = val.match(/^(\d{3,9})\s*[-–]\s*[A-Z]/i);
    if (m && !isYear(m[1])) {
      jobNo = m[1];
      break;
    }

    m = val.match(/^(\d{3,9})[-\s]/);
    if (m && !isYear(m[1])) {
      jobNo = m[1];
      break;
    }
  }

  // NOTE: previously had a fallback that pulled jobNo from "Purchase Order #<num>"
  // patterns. Removed — the PO number is NOT a job number. jobNo must come from
  // an explicit "Job #" / "Job No" label or line-items column. If missing, leave empty.

  // Fallback: scan raw text for NNNN-RETURN / NNNN-CREDIT patterns
  if (!jobNo) {
    const returnMatch = content.match(/\b(\d{3,9})[-–]\s*(?:RETURN|CREDIT|RMA|VOID)\b/i);
    if (returnMatch) jobNo = returnMatch[1];
  }

  // Collect ALL items tables (multi-page PDFs may produce separate tables per page)
  const itemsTables = [];

  for (const table of tables) {
    for (let r = 0; r < table.length; r++) {
      const upper = table[r].map(c => c.toUpperCase().trim());
      const tc = upper.findIndex(c => c === 'TOTAL' || c === 'AMOUNT' || c === 'EXT. PRICE' || c === 'EXT PRICE');
      const hasDesc = upper.some(c => c.includes('DESCRIPTION') || c.includes('PRODUCT') || c.includes('ITEM') || c.includes('SERVICE'));
      const hasPrice = upper.some(c => c.includes('PRICE') || c === 'RATE' || c === 'AMOUNT');

      if (tc >= 0 && (hasDesc || hasPrice)) {
        itemsTables.push({ table, headerRow: r });
        break;
      }
    }
  }

  let itemsSum = 0;
  let lastDesc = '';

  // Monetary values must have a decimal point — integers alone are line numbers or catalog codes
  const parseMonetary = (cell) => {
    const s = (cell || '').replace(/[$,]/g, '').replace(/\s*[-+]\s*$/, '').trim();
    if (!/\.\d+$/.test(s)) return null;  // must have at least one decimal digit (handles 2 or 4 dp)
    const n = parseFloat(s);
    // Cap at $100,000 — UPC codes like 783643156609.00 are not prices
    // Use absolute value — credit invoices output negative prices; sign is tracked via qty
    return (!isNaN(n) && n !== 0 && Math.abs(n) < 100000) ? Math.abs(n) : null;
  };

  // Detect line-number cell: small plain integer (≤ 999), no decimal
  const isLineNoCell = (cell) => /^\d{1,3}$/.test((cell || '').trim()) && parseInt(cell, 10) <= 999;

  for (const { table: itemsTable, headerRow: itemsHeaderRow } of itemsTables) {
    for (let ri = itemsHeaderRow + 1; ri < itemsTable.length; ri++) {
      const row = itemsTable[ri];

      // Strip leading LINE column if present so it never pollutes price parsing
      const firstCell = (row[0] || '').trim();
      const hasLineCol = isLineNoCell(firstCell);
      const lineNo = hasLineCol ? firstCell : '';
      const priceCells = hasLineCol ? row.slice(1) : row;

      const nums = priceCells.map(parseMonetary).filter(n => n !== null);

      if (nums.length === 0) {
        for (const cell of priceCells) {
          const cleaned = cell.replace(/\*\*\d+\*\*\s*/g, '').replace(/\s+\d+$/, '').trim();
          const isHeader = /^(LINE|QTY|PRODUCT|DESCRIPTION|PRICE|TOTAL|AMOUNT|UNIT|U\/M|DISCOUNT|SHIPPED|ORDERED|BACKORDERED|UPC|LIST|REFERENCE|REP|C\.O\.D|TAKEN BY|ORIGINAL INVOICE|SEE NOTES|SKU|CATALOG|CODE)/i.test(cleaned);
          if (!isHeader && cleaned.length > 4 && cleaned.length > lastDesc.length) lastDesc = cleaned;
        }
        continue;
      }

      const isFee = priceCells.some(c => /\bfee\b|surcharge|eco|levy/i.test(c));
      if (isFee) {
        const feeTotal = nums[nums.length - 1];
        const feeLabel = priceCells.find(c => /\bfee\b|surcharge|eco|levy/i.test(c)) || 'Fee';
        items.push({ lineNo, desc: feeLabel, qty: null, unit: null, total: feeTotal });
        itemsSum += feeTotal;
        continue;
      }

      const lineTotal = nums[nums.length - 1];
      const netPrice = nums.length >= 2 ? nums[nums.length - 2] : null;

      let desc = '';
      for (const cell of priceCells) {
        const cleaned = cell.replace(/\s+\d+$/, '').replace(/\s*[-+]\s*$/, '').trim();
        const isNumericOrCode =
          /^\$?-?[\d,.]+$/.test(cleaned) ||  // number or price (including negative like -721.35)
          cleaned.length === 0 ||
          /^(EA|EACH|PC|PCS|PR|FT|M|LB|KG|BOX|PKG|SET|LOT|RL|CTN|MT)$/i.test(cleaned) || // UoM
          /^\d{4,}$/.test(cleaned) ||         // catalog code (4+ digit integer)
          /^[A-Z][A-Z0-9]{3,19}$/.test(cleaned); // product code (all caps, no spaces, e.g. FLOX3)

        if (!isNumericOrCode && cleaned.length > desc.length) desc = cleaned;
      }

      if (!desc && lastDesc) desc = lastDesc;
      if (desc) lastDesc = desc;

      // Skip product-code sub-rows (e.g. GESCAN outputs "NPIFWSW ... 2.2440" before the real description row)
      // A product code row has: single monetary value + description looks like a product code (no spaces)
      if (nums.length === 1 && /^[A-Z0-9][A-Z0-9/.-]{2,19}$/.test(desc)) continue;

      // Qty: integer up to 4 digits; handle both -1 and 1- (trailing minus = accounting negative)
      let qty = null;
      for (let c = priceCells.length - 1; c >= 1; c--) {
        const cell = (priceCells[c] || '').trim();
        if (/^-?\d{1,4}-?$/.test(cell)) {
          const digits = cell.replace(/^-|-$/g, '');
          const sign = cell.startsWith('-') || cell.endsWith('-') ? -1 : 1;
          const n = parseInt(digits, 10) * sign;
          if (n !== 0 && n !== Math.round(lineTotal)) { qty = n; break; }
        }
      }

      // If qty still null but we have unit price and line total, derive qty mathematically
      if (qty === null && netPrice && netPrice > 0 && lineTotal > 0) {
        const derived = Math.round(lineTotal / netPrice);
        if (derived >= 1 && derived <= 9999 && Math.abs(derived * netPrice - lineTotal) < 0.02) {
          qty = derived;
        }
      }

      if (lineTotal > 0 && desc.length > 1) {
        items.push({ lineNo, desc, qty, unit: netPrice, total: lineTotal });
        itemsSum += lineTotal;
      }
    }
  }

  if (total === null) {
    const TOTAL_LABELS = [
      'GRAND TOTAL', 'INVOICE TOTAL', 'AMOUNT DUE', 'BALANCE DUE',
      'TOTAL DUE', 'TOTAL', 'SUBTOTAL', 'GROSS TOTAL'
    ];

    // Pick the largest absolute value across all candidates (tax-inclusive total > pre-tax subtotal)
    let bestAbs = 0;
    for (const lbl of TOTAL_LABELS) {
      const rawTotal = lmap[lbl];
      if (!rawTotal) continue;
      const n = parseFloat(rawTotal.replace(/[$,]/g, '').replace(/\s*[-+]\s*$/, ''));
      if (!isNaN(n) && Math.abs(n) > bestAbs) {
        bestAbs = Math.abs(n);
        total = n;
      }
    }
  }

  // If total looks like a single line item (< 60% of itemsSum), it's wrong — use itemsSum instead
  if (total !== null && itemsSum > 0 && Math.abs(total) < itemsSum * 0.6) {
    total = Math.round(itemsSum * 100) / 100;
  }

  if (total === null && itemsSum > 0) {
    total = Math.round(itemsSum * 100) / 100;
  }

  // ── ST-specific fields from lmap (fallback before JSON overlay) ──
  poNumber = getL(
    'PURCHASE ORDER NO', 'PURCHASE ORDER NUMBER', 'PURCHASE ORDER',
    'PO NO', 'PO NUMBER', 'PO #', 'P.O. NO', 'P.O. NUMBER', 'P.O. #',
    'YOUR ORDER', 'YOUR ORDER NO', 'YOUR ORDER NUMBER',
    'YOUR PURCHASE ORDER NUMBER', 'YOUR PURCHASE ORDER NO',  // EECOL
    'CUSTOMER PO', 'CUSTOMER P.O.', 'CUSTOMER ORDER',
    'CUSTOMER PO NUMBER', 'CUSTOMER P.O. NUMBER',            // Sinclair
    'CUSTOMER ORDER NO', 'CUSTOMER ORDER NUMBER'
  );

  requiredDate = parseDate(
    getL('REQUIRED DATE', 'DELIVERY DATE', 'SHIP DATE', 'NEED BY', 'NEED BY DATE')
  );

  vendorInvoiceNo = getL(
    'VENDOR INVOICE NO', 'VENDOR INVOICE NUMBER', 'VENDOR INVOICE'
  ) || invoiceNo || null;

  // ── JSON block overlay — highest priority for ST-specific fields ──
  let structured = null;
  const jsonSepMatch = content.match(/---JSON---\s*([\s\S]*?)(?:---|$)/);
  if (jsonSepMatch) {
    const raw = jsonSepMatch[1].trim();
    const objMatch = raw.match(/\{[\s\S]*\}/);
    try { structured = JSON.parse(objMatch ? objMatch[0] : raw); }
    catch(e) { console.warn('[fields] JSON block parse failed:', e.message); }
  }
  if (!structured) {
    const fenceMatch = content.match(/```json\s*([\s\S]*?)\s*```/);
    if (fenceMatch) {
      try { structured = JSON.parse(fenceMatch[1]); }
      catch(e) { /* skip */ }
    }
  }

  // Gemini-supplied tax/shipping — these are the source of truth when present,
  // since the regex/label fallbacks are easy to fool by document formatting.
  let geminiTax = null, geminiShipping = null;

  if (structured) {
    if (structured.vendorName)      vendor       = structured.vendorName      || vendor;
    if (structured.vendorInvoiceNo) { invoiceNo   = structured.vendorInvoiceNo; vendorInvoiceNo = structured.vendorInvoiceNo; }
    if (structured.poDate)          date         = parseDate(structured.poDate) || date;
    if (structured.totalAmount)     total        = structured.totalAmount;
    if (structured.poNumber)        poNumber     = structured.poNumber;
    if (structured.requiredDate)    requiredDate = parseDate(structured.requiredDate) || requiredDate;
    // Accept any finite number (including zero and negative) so credit notes
    // and return invoices propagate their negative tax/shipping through to ST.
    if (typeof structured.taxAmount === 'number' && Number.isFinite(structured.taxAmount))           geminiTax = structured.taxAmount;
    if (typeof structured.shippingAmount === 'number' && Number.isFinite(structured.shippingAmount)) geminiShipping = structured.shippingAmount;

    // Always prefer job number from line items — overrides any regex-derived guess
    if (Array.isArray(structured.lineItems) && structured.lineItems.length) {
      const lineJobNos = structured.lineItems
        .map(li => String(li.jobNo || '').trim())
        .filter(j => j && !isYear(j));
      if (lineJobNos.length > 0) {
        // Pick the most common value across all line items
        const counts = {};
        lineJobNos.forEach(j => { counts[j] = (counts[j] || 0) + 1; });
        const best = Object.keys(counts).sort((a, b) => counts[b] - counts[a])[0];
        if (best) jobNo = best;
      }
    }
    if (!jobNo && Array.isArray(structured.lineItems)) {
      for (const li of structured.lineItems) {
        const j = String(li.jobNo || '').trim();
        if (j && !isYear(j)) { jobNo = j; break; }
      }
    }

    if (Array.isArray(structured.lineItems) && structured.lineItems.length) {
      const structItems = structured.lineItems.filter(li => li.description || li.total);
      if (structItems.length) {
        items.length = 0;
        structItems.forEach(li => items.push({
          vendorPartNo: li.vendorPartNo || '',
          stPartNo:     li.stPartNo     || '',
          desc:         li.description  || '',
          jobNo:        li.jobNo        || '',
          qty:          li.quantity     || 1,
          unit:         li.cost         || null,
          total:        li.total        || 0
        }));
      }
    }
  }

  // ── Master-style detection ──
  // Supplier invoices that print a customer's order reference ("YOUR ORDER" /
  // "CUSTOMER PO" / "CUSTOMER ORDER"). These have a different shape from
  // Sasquatch-issued POs and Gescan-style invoices, so the rules below apply
  // ONLY to this format. Other PDF types keep all previous behavior.
  const customerOrderRef = getL(
    'YOUR ORDER', 'YOUR ORDER NO', 'YOUR ORDER NUMBER',
    'YOUR PURCHASE ORDER NUMBER', 'YOUR PURCHASE ORDER NO',  // EECOL
    'CUSTOMER PO', 'CUSTOMER P.O.', 'CUSTOMER ORDER',
    'CUSTOMER PO NUMBER', 'CUSTOMER P.O. NUMBER',            // Sinclair
    'CUSTOMER ORDER NO', 'CUSTOMER ORDER NUMBER'
  );
  const isMasterStyle = !!customerOrderRef;

  // ── Master-style: derive jobNo from customer's order reference ──
  // Examples:
  //   "67630170-001"   (Master, dash + sequence)        → 67630170
  //   "67964638nb"     (Sinclair, alphanumeric suffix)  → 67964638
  //   "45617786MC"     (EECOL, alphanumeric suffix)     → 45617786
  // Only when no explicit Job # was already extracted from line items.
  // Requires ≥6 leading digits so things like "01-062433" (invoice numbers)
  // and "2026" (years) don't get misread as jobs.
  if (isMasterStyle && !jobNo) {
    const m = String(customerOrderRef).trim().match(/^(\d{6,})/);
    if (m && !isYear(m[1])) jobNo = m[1];
  }

  // ── Andrew Sheret-style: derive jobNo / poNumber from "Notes" field ──
  // ASL invoices put the customer's PO/job reference in a "Notes" field as a
  // numeric prefix followed by metadata, e.g.:
  //   "67819132-MT-MS / MASON"   → 67819132
  //   "68025967/RYAN"            → 68025967
  // Requires ≥6 leading digits to avoid catching invoice numbers like "01-062433".
  const notesValue = getL('NOTES', 'NOTE');
  const isAndrewSheretStyle = !!(notesValue && /^\d{6,}\b/.test(String(notesValue).trim()));
  if (isAndrewSheretStyle && (!jobNo || !poNumber)) {
    const m = String(notesValue).trim().match(/^(\d{6,})\b/);
    if (m && !isYear(m[1])) {
      if (!jobNo)    jobNo    = m[1];
      if (!poNumber) poNumber = m[1];
    }
  }

  // ── Andrew Sheret-style: default requiredDate to invoice date ──
  // ASL invoices only print one date (in the "Information" row). Mirror it
  // into Required Date so both UI fields show the same value.
  if (isAndrewSheretStyle && !requiredDate && date) {
    requiredDate = date;
  }

  // ── Master-style: total = sum of line item totals (pre-tax subtotal) ──
  // Master invoices print both:    Total = 7962.41 (pre-tax subtotal)
  //                                Invoice Total = 8442.36 (with tax)
  // We can't reliably use lmap['TOTAL'] because the line items table
  // ALSO has a "TOTAL" column header — the label parser ends up pairing it
  // with the first line item's amount instead of the bottom subtotal.
  // Summing the line items directly gives the correct pre-tax subtotal.
  // ServiceTitan computes the post-tax PO total itself from items + tax + shipping.
  // sum !== 0 (not sum > 0) so credit notes / returns with negative subtotals
  // propagate correctly.
  if (isMasterStyle && Array.isArray(items) && items.length > 0) {
    const sum = items.reduce((s, li) => s + (parseFloat(li.total) || 0), 0);
    if (Number.isFinite(sum) && sum !== 0) total = Math.round(sum * 100) / 100;
  }

  // ── Master-style: default requiredDate to invoice date when not printed ──
  if (isMasterStyle && !requiredDate && date) {
    requiredDate = date;
  }

  // ── Master-style: recalculate effective unit cost from total / quantity ──
  // Master line items can have a DISC. % column where:
  //   PRICE=12551.00, DISC.%=64.00, TOTAL=4518.36 (effective cost per unit after discount)
  // EECOL line items can have a PER C column (per-100 pricing) where the displayed
  // UNIT PRICE doesn't multiply with quantity directly; the EXTENSION column already
  // bakes in the per-100 math.
  // ST has no per-line discount or per-100 field, so we send the discounted/effective
  // unit cost at 4-decimal precision — 2 dp rounding loses up to ~$1 per invoice on
  // high-qty PER C items (e.g. 250 × 0.48112 → 250 × 0.48 = 120.00, losing $0.28
  // versus the true $120.28). ST accepts 4 dp on the API even though the UI shows 2.
  // t !== 0 (not t > 0) so credit notes / returns with negative line totals
  // (e.g. Master "TOTAL 197.56-" meaning -$197.56) carry the discount through
  // instead of falling back to the gross PRICE column.
  if (isMasterStyle && items.length > 0) {
    items.forEach(li => {
      const t = parseFloat(li.total);
      const q = parseFloat(li.qty);
      if (Number.isFinite(t) && Number.isFinite(q) && q > 0 && t !== 0) {
        li.unit = Math.round((t / q) * 10000) / 10000;
      }
    });
  }

  // ── Plain-text total fallback (non-Master only) ──
  // Sasquatch/Gescan/Andrew Sheret PDFs may print totals as right-aligned text
  // outside tables. Master-style already had its total set above. The "[^\d.]{0,40}"
  // window allows arbitrary separator chars like ">>> $" (Andrew Sheret style)
  // while staying short enough to avoid spanning multiple labels.
  if (!isMasterStyle && (total === null || (itemsSum > 0 && Math.abs(total) < itemsSum * 0.9))) {
    const grandMatch =
      content.match(/\bInvoice\s+Total\b[^\d.]{0,40}([\d,]+\.\d{2})/i) ||
      content.match(/\bGrand\s+Total\b[^\d.]{0,40}([\d,]+\.\d{2})/i) ||
      content.match(/\bAmount\s+Due\b[^\d.]{0,40}([\d,]+\.\d{2})/i) ||
      content.match(/\bTotal\s+Due\b[^\d.]{0,40}([\d,]+\.\d{2})/i) ||
      content.match(/\bBalance\s+Due\b[^\d.]{0,40}([\d,]+\.\d{2})/i);
    if (grandMatch) {
      const n = parseFloat(grandMatch[1].replace(/,/g, ''));
      if (!isNaN(n) && n > 0 && (total === null || n > total)) total = n;
    }
  }

  // Extract tax amount — prefer Gemini's computed sum, otherwise fall back
  // to label/regex parsing.
  let tax = geminiTax;
  const TAX_LABELS = [
    'TOTAL TAX', 'TAX AMOUNT', 'SALES TAX', 'TAX',
    'GST', 'HST', 'PST', 'QST', 'VAT',
    'GST HST', 'GST/HST', 'G.S.T./H.S.T.', 'G.S.T. H.S.T.',
    'G.S.T.', 'H.S.T.', 'P.S.T.', 'Q.S.T.'
  ];
  // Only run the label-based sum if Gemini didn't already give us a tax value
  if (tax === null) {
    const seenTaxVals = new Set();
    for (const lbl of TAX_LABELS) {
      const rawTax = getL(lbl);
      if (rawTax) {
        const n = parseFloat(String(rawTax).replace(/[$,]/g, ''));
        if (!isNaN(n) && n >= 0 && !seenTaxVals.has(n)) {
          seenTaxVals.add(n);
          tax = (tax || 0) + n;
          // Stop early if we hit a labelled "TOTAL TAX" / "TAX AMOUNT" / "SALES TAX" — those are pre-summed
          if (/TOTAL TAX|TAX AMOUNT|SALES TAX/i.test(lbl)) break;
        }
      }
    }
  }

  // ── Plain-text tax fallback ──
  // Master-style invoices print tax lines as right-aligned plain text outside
  // any table. Strip HTML tags first so the regexes match uniformly regardless
  // of whether Gemini wrapped the totals block in a <table>. The patterns are
  // generous with dots and inner spacing so "G.S.T./H.S.T.", "GST/HST",
  // "G S T H S T", "PST", and "P.S.T." all match.
  if (tax === null || tax === 0) {
    const plain = content.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ');

    const matchTax = (re) => {
      const m = plain.match(re);
      if (!m) return 0;
      const n = parseFloat(m[1].replace(/,/g, ''));
      return Number.isFinite(n) && n > 0 ? n : 0;
    };

    let plainTax = 0;
    // The "[^.\n]{0,40}?" gap allows arbitrary non-newline chars between the
    // tax label and the amount — e.g. Sinclair prints "GST R104869441 1.61"
    // where the GST registration number sits between the label and the value.
    // The lazy quantifier ensures we don't span past the next tax line.

    // Combined GST/HST
    plainTax += matchTax(/G\s*\.?\s*S\s*\.?\s*T\s*\.?\s*\/\s*H\s*\.?\s*S\s*\.?\s*T\s*\.?[^.\n]{0,40}?([\d,]+\.\d{2})/i);
    if (plainTax === 0) {
      // Either alone
      plainTax += matchTax(/(?:^|[^A-Z])G\s*\.?\s*S\s*\.?\s*T\s*\.?(?!\s*\/)[^.\n]{0,40}?([\d,]+\.\d{2})/i);
      plainTax += matchTax(/(?:^|[^A-Z])H\s*\.?\s*S\s*\.?\s*T\s*\.?(?!\s*\/)[^.\n]{0,40}?([\d,]+\.\d{2})/i);
    }
    plainTax += matchTax(/(?:^|[^A-Z])P\s*\.?\s*S\s*\.?\s*T\s*\.?[^.\n]{0,40}?([\d,]+\.\d{2})/i);
    plainTax += matchTax(/(?:^|[^A-Z])Q\s*\.?\s*S\s*\.?\s*T\s*\.?[^.\n]{0,40}?([\d,]+\.\d{2})/i);

    if (plainTax > 0) tax = plainTax;
  }

  // Extract shipping/freight amount — prefer Gemini's value, otherwise label-based
  let shipping = geminiShipping;
  if (shipping === null) {
    for (const lbl of ['SHIPPING', 'FREIGHT', 'DELIVERY', 'SHIPPING & HANDLING', 'S&H', 'SHIPPING CHARGE', 'FREIGHT CHARGE']) {
      const rawShip = getL(lbl);
      if (rawShip) {
        const n = parseFloat(String(rawShip).replace(/[$,]/g, ''));
        if (!isNaN(n) && n >= 0) { shipping = n; break; }
      }
    }
  }

  // Final sanity pass: catch any year↔day swap that slipped through earlier
  // overlays / fallbacks (e.g. JSON overlay's structured.requiredDate after
  // fixSwappedYearDay was already applied to `date`).
  date         = fixSwappedYearDay(date);
  requiredDate = fixSwappedYearDay(requiredDate);

  console.log('[fields] extracted:', { vendor, invoiceNo, date, jobNo, total, tax, shipping, poNumber, requiredDate, itemCount: items.length });
  return { vendor, invoiceNo, date, jobNo, total, tax, shipping, items, poNumber, requiredDate, vendorInvoiceNo };
}

// ── Gemini SDK loader ──
let geminiClient = null;

async function getGeminiClient() {
  if (geminiClient) return geminiClient;

  const GEMINI_KEY = (process.env.GEMINI_API_KEY || '').trim();
  if (!GEMINI_KEY) throw new Error('GEMINI_API_KEY not set');

  const { GoogleGenAI } = await import('@google/genai');
  geminiClient = new GoogleGenAI({ apiKey: GEMINI_KEY });
  return geminiClient;
}

// ── Gemini helper using official SDK ──
async function parseWithGemini(fileBuffer, mimeType, model = GEMINI_MODEL) {
  const ai = await getGeminiClient();

  const prompt = `You are a receipt and invoice parser. Extract ALL content from this document exactly as printed.

Formatting rules (follow exactly):
- Output the SUPPLIER/ISSUER company name as a # H1 heading — this is the company that SENT or ISSUED the invoice (the seller), NOT the customer, bill-to party, or the company whose logo appears in a header watermark. For example, if "Sasquatch Heat Pumps" is printed at the top as the customer and "Andrew Sheret Limited" is the seller shown in the body, output "# Andrew Sheret Limited".
- "SOLD TO", "BILL TO", "SHIP TO", and "CUSTOMER" sections always describe the CUSTOMER (the recipient of the invoice). NEVER use any of these as the vendorName. The vendor is the company whose name/logo appears at the top of the document (e.g. "THE MASTER GROUP INC", "GESCAN LANGFORD", "ANDREW SHERET LIMITED").
- IGNORE watermark or stamp text like "*** DUPLICATE ***", "*** COPY ***", "ORIGINAL", "VOID", "PAID" — these are not data fields.
- Many invoices have a header table with column labels in one row and values in the row below (e.g. "CUSTOMER NO. | YOUR ORDER | CLERK | DATE | INVOICE" with values "99295 | 67630170-001 | Perrin Dixon | 11/05/26 | 74069953-00" beneath). Output these as a proper <table> with <tr><th> for labels and <tr><td> for values so each column pairs correctly.
- "YOUR ORDER" / "CUSTOMER PO" / "CUSTOMER ORDER" / "CUSTOMER PO NUMBER" / "YOUR PURCHASE ORDER NUMBER" is the customer's PO reference number — put it in poNumber. The value may be alphanumeric (e.g. "67964638nb", "45617786MC", "67630170-001").
- "INVOICE" / "INVOICE NO" / "INVOICE NUMBER" column on the supplier's invoice is the supplier's invoice number — put it in vendorInvoiceNo.
- The "Invoice Total" / "Grand Total" / "Amount Due" at the very bottom is the grand total — put that in totalAmount, not the pre-tax subtotal labelled "Total".
- Output every table as an HTML <table> with <tr><th> for header rows and <tr><td> for data cells
- Preserve every value exactly as printed — do not round numbers, reformat dates, or paraphrase
- Include ALL rows: header rows, sub-header rows, data rows, totals rows
- Output plain text (addresses, notes) as-is between tables
- Do not add commentary, explanations, or markdown code fences

CRITICAL rules — never break these:

LINE NUMBERS:
- The leftmost "LINE" or "LINE #" column contains sequential row identifiers (1, 2, 3, 9, 15…). These are NOT prices, NOT quantities, NOT totals.
- Always place line number integers in the LINE column cell only. Never put them in a PRICE, UNIT, UNIT PRICE, or TOTAL cell.
- A bare integer like "2" or "15" with no decimal point is always a line number or a quantity — it is NEVER a monetary amount.

PRICES AND TOTALS:
- Monetary values (unit price, extended price, total) always contain a decimal point with exactly 2 digits (e.g. $8.99, $1,348.47, $274.37).
- If a cell has no decimal point it is not a price. Do not invent or add decimal points.
- Never place a line number or a catalog code in a price column.
- The GRAND TOTAL or INVOICE TOTAL is ALWAYS the single largest dollar value at the very bottom of the document, after all line items. It is never a line item amount. Always include the totals row (TOTAL, GRAND TOTAL, SUBTOTAL, AMOUNT DUE) as a separate <tr> at the bottom of the table with the correct label in the first cell and the total value in the last cell.

QUANTITIES:
- The QTY, QTY ORDERED, QTY SHIPPED, or QUANTITY column contains whole numbers representing how many units (e.g. 1, 2, 5, 10, 47). These MUST be placed in the QTY column cell — never leave the QTY cell blank if a quantity is printed.
- Never confuse quantity with catalog/product codes. Quantities are small whole numbers (usually 1-999). Catalog codes are large numbers (4+ digits like 7150, 3520).
- A dash "—" or blank means the value is absent. Do not substitute.

UNIT PRICE:
- The UNIT PRICE, PRICE, or NET PRICE column contains the price per single unit with a decimal point (e.g. $3.40, $13.57, $36.85). Always extract this — never leave it blank if printed.
- Unit price is ALWAYS less than or equal to the line total (AMOUNT/EXT PRICE) for that row.

UNIT OF MEASURE:
- Unit of measure values (MT, EA, EACH, PC, FT, M, LB) belong in the U/M or UNIT column, not as a separate description row.

TRAILING MINUS / CREDIT NOTATION (STRICT):
- Some invoices (e.g. GESCAN) use trailing minus notation where the minus sign comes AFTER the number (e.g. "721.35-", "807.91-", "1-"). This means the value is negative.
- STRICT RULE: If a number has a trailing minus in the original document, you MUST output it as a negative number (e.g. "807.91-" → "-807.91"). Never drop the minus sign.
- STRICT RULE: If a number does NOT have a trailing minus or a leading minus in the original document, you MUST output it as a positive number. Never add a minus sign that is not printed.
- Apply this to ALL columns: QTY, NET PRICE, TOTAL, GROSS TOTAL, G.S.T., P.S.T., and the grand total.
- If the document is labelled "CREDIT", "RETURN", or "CREDIT - DO NOT PAY", verify all totals have trailing minus signs and output them as negative.

After all document content above, write a line containing only ---JSON--- then a single JSON object (no markdown fences, no extra text):
{"poNumber":"","poDate":"","requiredDate":"","vendorName":"","vendorInvoiceNo":"","totalAmount":0,"taxAmount":0,"shippingAmount":0,"lineItems":[{"vendorPartNo":"","stPartNo":"","description":"","jobNo":"","cost":0,"quantity":1,"total":0}]}
Fill every field from the document. Use empty string for missing text, 0 for missing numbers, YYYY-MM-DD for dates. For lineItems, include one entry per product/material row.
IMPORTANT: vendorName must be the company that ISSUED this invoice (the seller/supplier). Never use the customer name, bill-to name, or any watermark/logo company name that represents the recipient of the invoice.
IMPORTANT: taxAmount must be the SUM of every tax line on the document — GST, HST, PST, QST, VAT, sales tax, etc. For example, if the invoice prints "G.S.T./H.S.T. 398.12" AND "P.S.T. 81.83" as separate lines, taxAmount = 479.95 (the sum). If a single "Total Tax" or "Sales Tax" line is printed, use that. If no tax is shown, use 0.
IMPORTANT: shippingAmount is shipping/freight/delivery charges only. 0 if none.`;

  const withTimeout = (promise, ms, label) => {
    const timer = new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`Gemini ${label} timed out after ${ms / 1000}s`)), ms)
    );
    return Promise.race([promise, timer]);
  };

  if (mimeType === 'application/pdf') {
    // Upload via Files API so Gemini processes every page of the PDF
    const blob = new Blob([fileBuffer], { type: mimeType });
    const uploadedFile = await withTimeout(
      ai.files.upload({ file: blob, config: { mimeType, displayName: 'receipt.pdf' } }),
      60_000, 'file upload'
    );

    console.log('[gemini] uploaded PDF for multi-page processing, uri:', uploadedFile.uri);

    const response = await withTimeout(
      ai.models.generateContent({
        model,
        contents: [
          { text: prompt },
          { fileData: { mimeType, fileUri: uploadedFile.uri } }
        ]
      }),
      120_000, 'generateContent'
    );

    const text = response.text || '';
    if (!text) throw new Error('Gemini returned empty response');

    console.log(`[gemini] (${model}) output preview:`, text.substring(0, 300));
    return text;
  }

  // For images, use inlineData directly
  const response = await withTimeout(
    ai.models.generateContent({
      model,
      contents: [
        { text: prompt },
        {
          inlineData: {
            mimeType,
            data: fileBuffer.toString('base64')
          }
        }
      ]
    }),
    120_000, 'generateContent'
  );

  const text = response.text || '';
  if (!text) throw new Error('Gemini returned empty response');

  console.log(`[gemini] (${model}) output preview:`, text.substring(0, 300));
  return text;
}

async function parseWithGeminiRetry(fileBuffer, mimeType, model, maxRetries = 3) {
  let lastErr;
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await parseWithGemini(fileBuffer, mimeType, model);
    } catch (err) {
      if (/503|UNAVAILABLE|high demand|quota/i.test(err.message)) {
        lastErr = err;
        const delay = attempt * 10_000;
        console.warn(`[gemini] ${model} 503 on attempt ${attempt}/${maxRetries}, retrying in ${delay / 1000}s...`);
        await new Promise(r => setTimeout(r, delay));
      } else {
        throw err;
      }
    }
  }
  throw lastErr;
}

async function parseWithGeminiFallback(fileBuffer, mimeType) {
  try {
    return await parseWithGeminiRetry(fileBuffer, mimeType, GEMINI_MODEL);
  } catch (err) {
    if (/503|UNAVAILABLE|high demand|quota/i.test(err.message)) {
      console.warn(`[gemini] ${GEMINI_MODEL} exhausted retries, falling back to ${GEMINI_FALLBACK_MODEL}`);
      return await parseWithGeminiRetry(fileBuffer, mimeType, GEMINI_FALLBACK_MODEL);
    }
    throw err;
  }
}

app.post('/api/extract', upload.single('receipt'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    const fileBuffer = req.file.buffer;
    const mimeType = req.file.mimetype;

    console.log('[extract] mimetype:', mimeType, '| size:', fileBuffer.length, '| file:', req.file.originalname);

    const geminiOutput = await parseWithGeminiFallback(fileBuffer, mimeType);
    console.log('[extract] Gemini output preview:', geminiOutput.substring(0, 500));

    const imageDataUrl = mimeType !== 'application/pdf'
      ? `data:${mimeType};base64,${fileBuffer.toString('base64')}`
      : null;

    let receiptBlobUrl = null;
    if (process.env.BLOB_READ_WRITE_TOKEN) {
      try {
        const ext = mimeType === 'application/pdf' ? 'pdf' : (mimeType.split('/')[1] || 'jpg');
        const safeName = `receipt_${Date.now()}`;
        const blobResult = await blobPut(`receipts/${safeName}.${ext}`, fileBuffer, {
          access: 'public',
          contentType: mimeType,
          token: process.env.BLOB_READ_WRITE_TOKEN
        });
        receiptBlobUrl = blobResult.url;
        console.log('[extract] uploaded to Vercel Blob:', receiptBlobUrl);
      } catch (blobErr) {
        console.error('[extract] Blob upload failed:', blobErr.message);
      }
    }

    const fields = extractFieldsFromLlama(geminiOutput);

    return res.json({
      success: true,
      data: {
        markdown: geminiOutput,
        imageDataUrl,
        receiptBlobUrl,
        isPdf: mimeType === 'application/pdf',
        vendor:          fields.vendor          || null,
        invoiceNo:       fields.invoiceNo       || null,
        date:            fields.date            || null,
        total:           fields.total           || null,
        jobNo:           fields.jobNo           || null,
        jobStatus:       fields.jobNo ? 'found' : 'missing',
        items:           fields.items           || [],
        poNumber:        fields.poNumber        || null,
        requiredDate:    fields.requiredDate    || null,
        vendorInvoiceNo: fields.vendorInvoiceNo || null,
      }
    });
  } catch (err) {
    console.error('Extraction error:', err);

    if (err.code === 'LIMIT_FILE_SIZE') {
      return res.status(400).json({ error: 'File too large. Maximum size is 4MB.' });
    }

    return res.status(500).json({ error: err.message || 'Internal server error' });
  }
});

// ── Extract from URL (large files uploaded directly to Supabase Storage) ──
app.post('/api/extract-url', async (req, res) => {
  try {
    const { fileUrl, mimeType, originalName } = req.body || {};
    if (!fileUrl) return res.status(400).json({ error: 'No fileUrl provided' });

    const mType = (mimeType || 'application/pdf').trim();
    console.log('[extract-url] fetching:', originalName, '| mime:', mType);

    const fileRes = await fetch(fileUrl);
    if (!fileRes.ok) return res.status(400).json({ error: 'Could not download file' });

    const fileBuffer = Buffer.from(await fileRes.arrayBuffer());
    console.log('[extract-url] downloaded', fileBuffer.length, 'bytes');

    const geminiOutput = await parseWithGeminiFallback(fileBuffer, mType);
    const fields = extractFieldsFromLlama(geminiOutput);

    return res.json({
      success: true,
      data: {
        markdown: geminiOutput,
        imageDataUrl: null,
        receiptBlobUrl: fileUrl,
        isPdf: mType === 'application/pdf',
        vendor: fields.vendor || null,
        invoiceNo: fields.invoiceNo || null,
        date: fields.date || null,
        total: fields.total || null,
        jobNo: fields.jobNo || null,
        jobStatus: fields.jobNo ? 'found' : 'missing',
        items: fields.items || [],
      }
    });
  } catch (err) {
    console.error('[extract-url] error:', err);
    return res.status(500).json({ error: err.message || 'Internal server error' });
  }
});

// ── Jobber debug endpoint (visit /api/jobber-debug?job=1249 in browser) ──
app.get('/api/jobber-debug', async (req, res) => {
  try {
    const jobNum = req.query.job || '1249';
    const token = await getJobberToken();

    // Test 1: search
    const searchResult = await jobberGQL(`
      query { jobs(first: 5, searchTerm: "${jobNum}") { nodes { id jobNumber title } } }
    `);

    // Test 2: list first 5 jobs — try both nodes and edges
    const listResult = await jobberGQL(`
      query { jobs(first: 5) { nodes { id jobNumber title } edges { node { id jobNumber title } } pageInfo { hasNextPage endCursor } } }
    `);

    // Test 3: introspect Job type fields
    const schemaResult = await jobberGQL(`
      query { __type(name: "Job") { fields { name } } }
    `);

    return res.json({
      tokenExists: !!token,
      search: searchResult,
      list: listResult,
      jobFields: schemaResult?.data?.__type?.fields?.map(f => f.name) || schemaResult
    });
  } catch (err) {
    return res.json({ error: err.message });
  }
});

// ── Health check ──
app.get('/api/health', (req, res) => {
  return res.json({
    status: 'ok',
    version: 'v5',
    model: GEMINI_MODEL,
    jobberConfigured: !!(
      (process.env.JOBBER_CLIENT_ID || '').trim() &&
      (process.env.JOBBER_CLIENT_SECRET || '').trim()
    ),
    blobConfigured: !!(process.env.BLOB_READ_WRITE_TOKEN || '').trim()
  });
});

// ── Upstash Redis helpers ──
async function redisGet(key) {
  if (!process.env.UPSTASH_REDIS_REST_URL) return null;

  const r = await fetch(`${process.env.UPSTASH_REDIS_REST_URL}/get/${key}`, {
    headers: { Authorization: `Bearer ${process.env.UPSTASH_REDIS_REST_TOKEN}` }
  });

  const data = await r.json();
  return data.result || null;
}

async function redisDel(key) {
  if (!process.env.UPSTASH_REDIS_REST_URL) return;
  await fetch(`${process.env.UPSTASH_REDIS_REST_URL}/del/${key}`, {
    headers: { Authorization: `Bearer ${process.env.UPSTASH_REDIS_REST_TOKEN}` }
  });
}

async function redisSet(key, value, exSeconds) {
  if (!process.env.UPSTASH_REDIS_REST_URL) return;

  let url = `${process.env.UPSTASH_REDIS_REST_URL}/set/${key}/${encodeURIComponent(value)}`;
  if (exSeconds) url += `/ex/${exSeconds}`;

  await fetch(url, {
    headers: { Authorization: `Bearer ${process.env.UPSTASH_REDIS_REST_TOKEN}` }
  });
}

// ── Jobber token management ──
async function getJobberToken() {
  let token = await redisGet('jobber_access_token');
  if (token) return token;

  const refreshToken = await redisGet('jobber_refresh_token');
  if (!refreshToken) throw new Error('NOT_CONNECTED');

  const res = await fetch('https://api.getjobber.com/api/oauth/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      client_id: (process.env.JOBBER_CLIENT_ID || '').trim(),
      client_secret: (process.env.JOBBER_CLIENT_SECRET || '').trim(),
      refresh_token: refreshToken,
      grant_type: 'refresh_token'
    })
  });

  const tokens = await res.json();
  if (!tokens.access_token) throw new Error('TOKEN_REFRESH_FAILED');

  const ttl = Math.max((tokens.expires_in || 3600) - 300, 300); // 5-min buffer
  await redisSet('jobber_access_token', tokens.access_token, ttl);
  if (tokens.refresh_token) await redisSet('jobber_refresh_token', tokens.refresh_token);

  return tokens.access_token;
}

// ── Jobber GraphQL helper ──
async function jobberGQL(query, variables = {}, _retry = false) {
  const token = await getJobberToken();

  const res = await fetch('https://api.getjobber.com/api/graphql', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
      'X-JOBBER-GRAPHQL-VERSION': '2026-03-10'
    },
    body: JSON.stringify({ query, variables })
  });

  const json = await res.json();

  // Token expired in Jobber but Redis still had it — clear and retry once
  if (!_retry && json.message === 'Access token expired') {
    console.log('[jobber] access token expired, clearing cache and retrying...');
    await redisDel('jobber_access_token');
    return jobberGQL(query, variables, true);
  }

  if (json.errors?.length) console.log('[jobberGQL] errors:', JSON.stringify(json.errors));
  return json;
}

// ── Jobber auth routes ──
app.get('/api/auth/jobber', (req, res) => {
  const appUrl = (process.env.APP_URL || '').trim().replace(/\/$/, '');
  if (!appUrl) return res.status(500).send('APP_URL environment variable not set');

  const url = new URL('https://api.getjobber.com/api/oauth/authorize');
  url.searchParams.set('client_id', (process.env.JOBBER_CLIENT_ID || '').trim());
  url.searchParams.set('redirect_uri', `${appUrl}/api/auth/callback`);
  url.searchParams.set('response_type', 'code');

  return res.redirect(url.toString());
});

app.get('/api/auth/callback', async (req, res) => {
  const { code } = req.query;
  if (!code) return res.status(400).send('Missing authorization code');

  try {
    const appUrl = (process.env.APP_URL || '').trim().replace(/\/$/, '');

    const tokenRes = await fetch('https://api.getjobber.com/api/oauth/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        client_id: (process.env.JOBBER_CLIENT_ID || '').trim(),
        client_secret: (process.env.JOBBER_CLIENT_SECRET || '').trim(),
        code,
        grant_type: 'authorization_code',
        redirect_uri: `${appUrl}/api/auth/callback`
      })
    });

    const tokens = await tokenRes.json();

    if (!tokens.access_token) {
      return res.status(400).send('Failed to get token: ' + JSON.stringify(tokens));
    }

    const cbTtl = Math.max((tokens.expires_in || 3600) - 300, 300);
    await redisSet('jobber_access_token', tokens.access_token, cbTtl);
    await redisSet('jobber_refresh_token', tokens.refresh_token);

    return res.send(`<!DOCTYPE html><html><head><meta charset="UTF-8"><title>Connected!</title></head>
      <body style="font-family:system-ui;text-align:center;padding:60px;background:#F7F8FA;">
        <div style="background:#fff;border:1px solid #E5E7EB;border-radius:12px;padding:40px;max-width:400px;margin:0 auto;">
          <div style="color:#059669;font-size:48px;margin-bottom:16px;">&#10003;</div>
          <h2 style="margin:0 0 8px;color:#111827;">Connected to Jobber!</h2>
          <p style="color:#6B7280;margin:0 0 24px;">ReceiptFlow can now create expenses in your Jobber account.</p>
          <a href="/" style="background:#B8620A;color:#fff;padding:10px 20px;border-radius:6px;text-decoration:none;font-weight:600;">Return to ReceiptFlow</a>
        </div>
      </body></html>`);
  } catch (err) {
    return res.status(500).send('Error: ' + err.message);
  }
});

app.get('/api/auth/status', async (req, res) => {
  try {
    const token = await redisGet('jobber_access_token');
    const refresh = await redisGet('jobber_refresh_token');
    return res.json({ connected: !!(token || refresh) });
  } catch {
    return res.json({ connected: false });
  }
});

// ── Frontend-compatible Jobber status route ──
app.get('/api/jobber-status', async (req, res) => {
  try {
    const token = await redisGet('jobber_access_token');
    const refresh = await redisGet('jobber_refresh_token');

    const hasClientId = !!(process.env.JOBBER_CLIENT_ID || '').trim();
    const hasClientSecret = !!(process.env.JOBBER_CLIENT_SECRET || '').trim();
    const appUrl = (process.env.APP_URL || '').trim().replace(/\/$/, '');

    let authUrl = null;
    if (hasClientId && hasClientSecret && appUrl) {
      const url = new URL('https://api.getjobber.com/api/oauth/authorize');
      url.searchParams.set('client_id', (process.env.JOBBER_CLIENT_ID || '').trim());
      url.searchParams.set('redirect_uri', `${appUrl}/api/auth/callback`);
      url.searchParams.set('response_type', 'code');
      authUrl = url.toString();
    }

    return res.json({
      connected: !!(token || refresh),
      hasClientId,
      hasClientSecret,
      authUrl
    });
  } catch {
    return res.json({
      connected: false,
      hasClientId: !!(process.env.JOBBER_CLIENT_ID || '').trim(),
      hasClientSecret: !!(process.env.JOBBER_CLIENT_SECRET || '').trim(),
      authUrl: null
    });
  }
});

// ── Process a single queue item (called per-file from the frontend with JWT auth) ──
app.post('/api/process-queue-item', async (req, res) => {
  try {
    const { queueId } = req.body || {};
    const sb = await getSupabaseAdmin();

    let row;
    if (queueId) {
      const { data } = await sb.from('upload_queue').select('*')
        .eq('id', queueId).eq('user_id', req.user.id).eq('status', 'pending').single();
      row = data;
    } else {
      const { data } = await sb.from('upload_queue').select('*')
        .eq('user_id', req.user.id).eq('status', 'pending')
        .order('created_at', { ascending: true }).limit(1).single();
      row = data;
    }

    if (!row) return res.json({ success: true, message: 'No pending item found' });

    const { data: claimed } = await sb.from('upload_queue')
      .update({ status: 'processing' })
      .eq('id', row.id).eq('status', 'pending').select('id');

    if (!claimed || claimed.length === 0) return res.json({ success: true, message: 'Item already claimed' });

    const result = await processOneQueueRow(sb, row);
    return res.json({ success: true, result });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

// ── ServiceTitan shared helpers ──
function getSTCreds() {
  const creds = {
    tenantId:     (process.env.ST_TENANT_ID     || '').trim(),
    appKey:       (process.env.ST_APP_KEY       || '').trim(),
    clientId:     (process.env.ST_CLIENT_ID     || '').trim(),
    clientSecret: (process.env.ST_CLIENT_SECRET || '').trim()
  };
  if (!creds.tenantId || !creds.appKey || !creds.clientId || !creds.clientSecret) return null;
  return creds;
}

async function getSTToken(creds) {
  const res = await fetch('https://auth.servicetitan.io/connect/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type:    'client_credentials',
      client_id:     creds.clientId,
      client_secret: creds.clientSecret
    })
  });
  const data = await res.json();
  if (!data.access_token) throw new Error('ST auth failed: ' + (data.error_description || data.error || 'unknown'));
  return data.access_token;
}

async function lookupSTVendor(token, creds, vendorName) {
  const h = { 'Authorization': 'Bearer ' + token, 'ST-App-Key': creds.appKey };
  const base = `https://api.servicetitan.io/inventory/v2/tenant/${creds.tenantId}/vendors`;

  async function searchVendors(params) {
    try {
      const r = await fetch(`${base}?` + new URLSearchParams({ pageSize: '200', active: 'true', ...params }), { headers: h });
      if (!r.ok) return [];
      const d = await r.json();
      return d.data || d.items || [];
    } catch { return []; }
  }

  // Always fetch the full list for both matching and error reporting
  const all = await searchVendors({});
  const allNames = all.map(v => v.name).filter(Boolean);

  if (!vendorName) return { vendorId: null, availableVendors: allNames };

  const nameLower = vendorName.toLowerCase();

  // 1. Exact name match (case-insensitive)
  const exactMatch = all.find(v => (v.name || '').toLowerCase() === nameLower);
  if (exactMatch) {
    console.log(`[st-vendor] exact match "${vendorName}" → id=${exactMatch.id}`);
    return { vendorId: exactMatch.id, availableVendors: allNames };
  }

  // 2. Contains match — vendor name contains the extracted name or vice versa
  const containsMatch = all.find(v => {
    const vn = (v.name || '').toLowerCase();
    return vn.includes(nameLower) || nameLower.includes(vn);
  });
  if (containsMatch) {
    console.log(`[st-vendor] contains match "${vendorName}" → "${containsMatch.name}" id=${containsMatch.id}`);
    return { vendorId: containsMatch.id, availableVendors: allNames };
  }

  // 3. Word-overlap fuzzy match
  const words = nameLower.split(/\s+/).filter(w => w.length > 2);
  let best = null, bestScore = 0;
  for (const v of all) {
    const vn = (v.name || '').toLowerCase();
    const score = words.filter(w => vn.includes(w)).length;
    if (score > bestScore) { bestScore = score; best = v; }
  }
  if (best && bestScore > 0) {
    console.log(`[st-vendor] fuzzy match "${vendorName}" → "${best.name}" id=${best.id} (score=${bestScore})`);
    return { vendorId: best.id, availableVendors: allNames };
  }

  console.warn(`[st-vendor] no match for "${vendorName}" | ST has: ${allNames.join(', ')}`);
  return { vendorId: null, availableVendors: allNames };
}

// Search ST pricebook for a matching SKU by vendor part number, then description
async function lookupSTSku(token, creds, vendorId, vendorPartNo, description) {
  const h = { 'Authorization': 'Bearer ' + token, 'ST-App-Key': creds.appKey };
  const base = 'https://api.servicetitan.io';
  const tid = creds.tenantId;

  async function fetchData(url) {
    try {
      const r = await fetch(url, { headers: h });
      if (!r.ok) return null;
      const d = await r.json();
      return d.data || d.items || [];
    } catch { return null; }
  }

  const partNo = (vendorPartNo || '').trim();
  if (!partNo || partNo.length < 2 || partNo === 'N/A') {
    console.log(`[sku-lookup] empty/invalid partNo — skipping`);
    return null;
  }
  const partLower = partNo.toLowerCase();

  // Strategy 1: most ST pricebooks store the vendor part number as the SKU's
  // `code` field. The API supports filtering by code= exactly. Try this first.
  const byCode = await fetchData(
    `${base}/pricebook/v2/tenant/${tid}/materials?` +
    new URLSearchParams({ code: partNo, pageSize: '25', active: 'true' })
  );
  if (Array.isArray(byCode) && byCode.length > 0) {
    const exact = byCode.find(m => (m.code || '').toLowerCase() === partLower);
    if (exact) {
      console.log(`[sku-lookup] matched by code: id=${exact.id} code="${exact.code}" for partNo="${partNo}"`);
      return { skuId: exact.id, vendorPartNumber: exact.code || partNo };
    }
  }

  // Strategy 2: scan materials for this vendor and look for the part number
  // inside vendors[].vendorPartNumber (ST allows mapping multiple vendor part
  // numbers to one internal SKU code). Page through up to ~400 materials so
  // a typical mid-size pricebook is covered without being slow.
  if (vendorId) {
    let page = 1;
    while (page <= 4) {
      const vendorMats = await fetchData(
        `${base}/pricebook/v2/tenant/${tid}/materials?` +
        new URLSearchParams({ vendorId: String(vendorId), pageSize: '100', page: String(page), active: 'true' })
      );
      if (!Array.isArray(vendorMats) || vendorMats.length === 0) break;
      const match = vendorMats.find(m => {
        if ((m.code || '').toLowerCase() === partLower) return true;
        if (Array.isArray(m.vendors)) {
          return m.vendors.some(v => (v.vendorPartNumber || '').toLowerCase() === partLower);
        }
        return false;
      });
      if (match) {
        console.log(`[sku-lookup] matched in vendor's materials (page ${page}): id=${match.id} for partNo="${partNo}"`);
        return { skuId: match.id, vendorPartNumber: match.code || partNo };
      }
      if (vendorMats.length < 100) break;
      page++;
    }
  }

  console.log(`[sku-lookup] no SKU in ST pricebook matches partNo="${partNo}" — falling back to default`);
  return null;
}

async function getSTLookups(token, creds) {
  const h = { 'Authorization': 'Bearer ' + token, 'ST-App-Key': creds.appKey };
  const base = 'https://api.servicetitan.io';
  const tid = creds.tenantId;

  async function firstId(...urls) {
    for (const url of urls) {
      try {
        const r = await fetch(url, { headers: h });
        if (!r.ok) continue;
        const d = await r.json();
        const items = d.data || d.items || [];
        if (items[0]?.id) return items[0].id;
      } catch { /* try next */ }
    }
    return null;
  }

  // Search all location-type endpoints for a name match (case-insensitive, exact then contains)
  async function findLocationByName(name) {
    if (!name) return null;
    const targetLower = name.toLowerCase().trim();
    const endpoints = [
      `${base}/inventory/v2/tenant/${tid}/inventory-locations?active=true&pageSize=200`,
      `${base}/inventory/v2/tenant/${tid}/trucks?active=true&pageSize=200`,
      `${base}/inventory/v2/tenant/${tid}/locations?active=true&pageSize=200`
    ];
    for (const url of endpoints) {
      try {
        const r = await fetch(url, { headers: h });
        if (!r.ok) continue;
        const d = await r.json();
        const items = d.data || d.items || [];
        const exact = items.find(it => (it.name || '').toLowerCase().trim() === targetLower);
        if (exact?.id) return exact.id;
        const partial = items.find(it => (it.name || '').toLowerCase().includes(targetLower));
        if (partial?.id) return partial.id;
      } catch { /* try next */ }
    }
    return null;
  }

  const typeId = parseInt(process.env.ST_TYPE_ID || '') ||
    await firstId(`${base}/inventory/v2/tenant/${tid}/purchase-order-types?active=true&pageSize=1`);

  // ST uses different namespaces across accounts — try all known paths
  const businessUnitId = parseInt(process.env.ST_BU_ID || '') ||
    await firstId(
      `${base}/businessunits/v2/tenant/${tid}/business-units?active=true&pageSize=1`,
      `${base}/settings/v2/tenant/${tid}/business-units?active=true&pageSize=1`,
      `${base}/tenant/v2/tenant/${tid}/business-units?active=true&pageSize=1`
    );

  // Location resolution priority:
  //   1. ST_LOCATION_ID env var (explicit override)
  //   2. ST_LOCATION_NAME env var match (default "Default Warehouse")
  //   3. First available location from any endpoint
  const locationName = (process.env.ST_LOCATION_NAME || 'Default Warehouse').trim();
  let locationId = parseInt(process.env.ST_LOCATION_ID || '') || null;
  let locationSource = 'env-id';
  if (!locationId && locationName) {
    locationId = await findLocationByName(locationName);
    if (locationId) locationSource = `name="${locationName}"`;
  }
  if (!locationId) {
    locationId = await firstId(
      `${base}/inventory/v2/tenant/${tid}/inventory-locations?active=true&pageSize=1`,
      `${base}/inventory/v2/tenant/${tid}/trucks?active=true&pageSize=1`,
      `${base}/inventory/v2/tenant/${tid}/locations?active=true&pageSize=1`
    );
    if (locationId) locationSource = 'first-available';
  }

  // Return Type — only needed when posting Return Receipts (credit notes).
  // Auto-discover from ST so we don't depend on a hardcoded ID that the
  // tenant may or may not have. ST_RETURN_TYPE_ID can still override.
  const returnTypeId = parseInt(process.env.ST_RETURN_TYPE_ID || '') ||
    await firstId(
      `${base}/inventory/v2/tenant/${tid}/return-types?active=true&pageSize=1`,
      `${base}/inventory/v2/tenant/${tid}/returns/types?active=true&pageSize=1`,
      `${base}/inventory/v2/tenant/${tid}/return-receipt-types?active=true&pageSize=1`
    );

  console.log(`[st-lookups] typeId=${typeId} buId=${businessUnitId} locationId=${locationId} (${locationSource}) returnTypeId=${returnTypeId || 'none'}`);
  return { typeId, businessUnitId, locationId, returnTypeId };
}

async function lookupSTJob(token, creds, jobNo) {
  if (!jobNo) return null;
  const h = { 'Authorization': 'Bearer ' + token, 'ST-App-Key': creds.appKey };
  const tid = creds.tenantId;
  const base = 'https://api.servicetitan.io';

  // Try direct ID lookup first (fast path when extracted number IS the ST job ID)
  const parsed = parseInt(jobNo);
  if (parsed) {
    try {
      const r = await fetch(`${base}/jpm/v2/tenant/${tid}/jobs/${parsed}`, { headers: h });
      if (r.ok) {
        const d = await r.json();
        if (d.id) return d.id;
      }
    } catch { /* fall through */ }
  }

  // Try searching by job number field
  try {
    const r = await fetch(
      `${base}/jpm/v2/tenant/${tid}/jobs?` + new URLSearchParams({ number: jobNo, pageSize: '1' }),
      { headers: h }
    );
    if (r.ok) {
      const d = await r.json();
      const items = d.data || d.items || [];
      if (items[0]?.id) return items[0].id;
    }
  } catch { /* fall through */ }

  return null;
}

async function createSTPurchaseOrder({ poNumber, vendor, vendorInvoiceNo, date, requiredDate, tax, shipping, jobId, lineItems, total }) {
  const creds = getSTCreds();
  if (!creds) throw new Error('ServiceTitan credentials not configured');

  const token = await getSTToken(creds);

  // ── Run all ST lookups in parallel ──
  const t0 = Date.now();
  const [lookups, vendorResult, resolvedJobId] = await Promise.all([
    getSTLookups(token, creds),
    lookupSTVendor(token, creds, vendor),
    lookupSTJob(token, creds, jobId ? String(jobId) : null)
  ]);
  console.log(`[create-po] parallel ST lookups done in ${Date.now() - t0}ms`);

  const businessUnitId      = parseInt(process.env.ST_BU_ID       || '') || lookups.businessUnitId;
  const inventoryLocationId = parseInt(process.env.ST_LOCATION_ID || '') || lookups.locationId;
  const typeId              = parseInt(process.env.ST_TYPE_ID     || '') || lookups.typeId;

  if (!businessUnitId)      throw new Error('No business unit found — set ST_BU_ID in .env or visit /api/test-st');
  if (!inventoryLocationId) throw new Error('No inventory location found — set ST_LOCATION_ID in .env or visit /api/test-st');
  if (!typeId)              throw new Error('No PO type found — set ST_TYPE_ID in .env or visit /api/test-st');

  const { vendorId, availableVendors } = vendorResult;
  console.log(`[create-po] vendor="${vendor}" → vendorId=${vendorId} | ST vendors: ${availableVendors.join(', ')}`);
  if (!vendorId) {
    throw new Error(
      `Vendor "${vendor}" not found in ServiceTitan. ` +
      `Available vendors: ${availableVendors.length ? availableVendors.join(', ') : '(none found)'}. ` +
      `Add this vendor to ST first (Inventory → Vendors) then retry.`
    );
  }

  // ── Date from receipt PDF, fall back to today ──
  const today = new Date().toISOString().slice(0, 10);
  const safeDate = (s) => {
    if (!s) return null;
    s = String(s).trim();
    // Already ISO YYYY-MM-DD — pass through without going through new Date()
    // (which can mis-interpret depending on locale).
    if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
    try { return new Date(s).toISOString().slice(0, 10); } catch { return null; }
  };

  // If a parsed date is wildly off from today (>2 years past or >1 year future),
  // it's almost certainly a YY/DD swap from 2-digit-year extraction (e.g.
  // "22/05/26" parsed by Gemini as 26/05/22 → 2022-05-26 instead of 2026-05-22).
  // Detect and try the swap; pick whichever lands closer to today.
  const fixSwappedYearDay = (iso) => {
    if (!iso) return iso;
    const m = iso.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (!m) return iso;
    const year = parseInt(m[1], 10);
    const day  = parseInt(m[3], 10);
    const currentYear = parseInt(today.slice(0, 4), 10);

    // Only attempt swap when year looks suspicious AND swapping yields a
    // plausible date (year=2000+old_day in current era, day=old_year-2000 ≤ 31).
    if (year < currentYear - 2 || year > currentYear + 2) {
      const newYearLast2 = day;
      const newDay      = year >= 2000 ? year - 2000 : year - 1900;
      const newYear     = newYearLast2 < 70 ? 2000 + newYearLast2 : 1900 + newYearLast2;
      if (newDay >= 1 && newDay <= 31 && Math.abs(newYear - currentYear) < Math.abs(year - currentYear)) {
        const swapped = `${newYear}-${m[2]}-${String(newDay).padStart(2,'0')}`;
        console.warn(`[create-po] suspicious date ${iso} (year off by ${Math.abs(year - currentYear)}); swapping year↔day → ${swapped}`);
        return swapped;
      }
    }
    return iso;
  };

  const poDate        = fixSwappedYearDay(safeDate(date)) || today;
  const rawRequiredOn = fixSwappedYearDay(safeDate(requiredDate)) || poDate;
  // ST rejects requiredOn before the PO creation date (today)
  const poRequiredOn = rawRequiredOn < today ? today : rawRequiredOn;
  console.log(`[create-po] date input="${date}" → poDate="${poDate}" | requiredDate input="${requiredDate}" → poRequiredOn="${poRequiredOn}"`);

  // ── shipTo — CreateShipToRequest: { description, address: CreateAddressRequest } ──
  const shipTo = {
    description: (process.env.ST_SHIP_TO_DESCRIPTION || 'Main Location').trim(),
    address: {
      street:  (process.env.ST_SHIP_TO_STREET  || '').trim(),
      unit:    (process.env.ST_SHIP_TO_UNIT    || '').trim(),
      city:    (process.env.ST_SHIP_TO_CITY    || '').trim(),
      state:   (process.env.ST_SHIP_TO_STATE   || '').trim(),
      zip:     (process.env.ST_SHIP_TO_ZIP     || '').trim(),
      country: (process.env.ST_SHIP_TO_COUNTRY || 'CA').trim()
    }
  };

  const isCredit = isCreditNote({ total, tax, lineItems });
  const absNum = v => Math.abs(parseFloat(v) || 0);
  const defaultSkuId = parseInt(process.env.ST_DEFAULT_SKU_ID || '0') || 0;
  const stBase = 'https://api.servicetitan.io';
  const stHeaders = {
    'Authorization': 'Bearer ' + token,
    'ST-App-Key':    creds.appKey,
    'Content-Type':  'application/json'
  };

  // Helper: build a single ST line item (handles SKU lookup + cost/qty shape).
  const buildSTItem = async (li, opts = {}) => {
    const sku = await lookupSTSku(token, creds, vendorId, li.vendorPartNo || li.stPartNo, li.desc);
    const desc = (li.desc || vendor || 'Item').trim();
    return {
      skuId:            sku ? sku.skuId : defaultSkuId,
      vendorPartNumber: sku ? sku.vendorPartNumber : ((li.vendorPartNo || li.stPartNo || 'N/A').trim() || 'N/A'),
      description:      opts.descPrefix ? `${opts.descPrefix} ${desc}`.slice(0, 500) : desc,
      quantity:         Math.max(1, parseFloat(li.qty) || 1),
      // ST requires cost > 0; pass absolute value (sign is encoded by which
      // document we post to — PO for charges, Return Receipt for returns).
      cost:             Math.round(absNum(li.unit) * 10000) / 10000
    };
  };

  // Helper: post a payload to one of ST's inventory endpoints. Returns parsed
  // JSON or throws with a useful error string.
  const postToST = async (urlPath, body, logTag) => {
    console.log(`[${logTag}] payload:`, JSON.stringify(body));
    const res = await fetch(`${stBase}${urlPath}`, { method: 'POST', headers: stHeaders, body: JSON.stringify(body) });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      const detail = data.errors ? JSON.stringify(data.errors) : (data.detail || data.title || JSON.stringify(data));
      console.error(`[${logTag}] ST error ${res.status}:`, JSON.stringify(data));
      throw new Error(detail);
    }
    return data;
  };

  // ── Helper to build the common PO/Return Receipt scaffold ──
  const baseBody = (extra = {}) => {
    const body = {
      typeId,
      date:                     poDate,
      vendorId:                 vendorId || undefined,
      businessUnitId,
      inventoryLocationId,
      shipTo,
      impactsTechnicianPayroll: false,
      ...extra
    };
    if (resolvedJobId) body.jobId = resolvedJobId;
    return body;
  };

  if (isCredit) {
    // ── Credit note path ──
    // Two sub-paths depending on whether the tenant has a Return Type
    // configured (required by ST's /returns endpoint).
    //
    // 1. WITH Return Type: split charges → PO, returns → Return Receipt.
    //    Net result matches the source invoice exactly.
    //
    // 2. WITHOUT Return Type: post ALL line items on a single PO with
    //    absolute costs, tagged [CHARGE]/[RETURN] in the description.
    //    Every item is still visible in ST, just on one document. The
    //    sub-total will be the sum of absolutes (overstated relative to
    //    the signed net) — this is the only way to keep all items
    //    visible when Return Receipt posting isn't available.

    const positiveLines = (lineItems || []).filter(li => (parseFloat(li.total) || 0) > 0);
    const negativeLines = (lineItems || []).filter(li => (parseFloat(li.total) || 0) < 0);

    const memoBase = `Vendor Invoice: ${vendorInvoiceNo || 'n/a'}`;
    const origTotalStr = Number.isFinite(parseFloat(total)) ? ` (Original Invoice Total: $${parseFloat(total).toFixed(2)})` : '';

    const returnTypeId  = parseInt(process.env.ST_RETURN_TYPE_ID || '') || lookups.returnTypeId;
    const restockingFee = parseFloat(process.env.ST_RESTOCKING_FEE || '0') || 0;

    // ── FALLBACK PATH: no Return Type in tenant ──
    // Post a single synthetic PO line carrying the NET absolute amount so
    // the ST total matches |Invoice Total|. The line description lists every
    // original item (charge and return) with its real signed amount, so the
    // user can still see every item that was on the source credit note. SKU
    // comes from the first looked-up real item, so ST's "Material assigned"
    // validation passes.
    if (negativeLines.length > 0 && !returnTypeId) {
      console.warn(`[create-po] credit note + no Return Type configured — posting one synthetic line with net absolute total`);

      // Look up SKU for at least one real item so ST accepts the line.
      const lineSkus = await Promise.all((lineItems || []).map(li =>
        lookupSTSku(token, creds, vendorId, li.vendorPartNo || li.stPartNo, li.desc)
      ));
      const firstSku = lineSkus.find(s => s && s.skuId);
      if (!firstSku && !defaultSkuId) {
        throw new Error(
          `Cannot post credit note: none of the line items (${(lineItems || []).map(li => li.vendorPartNo || '?').join(', ')}) ` +
          `match a SKU in ST. Add the part numbers in ST or set ST_DEFAULT_SKU_ID.`
        );
      }

      // Compute |Invoice Total| from line sum + tax (so ST line total negation
      // produces exactly the source invoice's |Invoice Total|).
      const netLineSum  = (lineItems || []).reduce((s, li) => s + (parseFloat(li.total) || 0), 0);
      const totalAbs    = Math.abs(netLineSum) + absNum(tax);
      const totalAbsRnd = Math.round(totalAbs * 100) / 100 || 0.01;

      const itemSummary = (lineItems || []).map(li => {
        const t = parseFloat(li.total) || 0;
        const tag = t < 0 ? 'RETURN' : 'CHARGE';
        const part = li.vendorPartNo || li.desc || 'item';
        return `${tag} ${part} (${t < 0 ? '-' : ''}$${Math.abs(t).toFixed(2)})`;
      }).join('; ');

      // Experiment: try qty=-1 first so ST shows a negative line total
      // (= negative PO total). If ST rejects negative qty (likely — POs are
      // for charges only), we retry with qty=1 to at least get the receipt
      // posted as a positive credit-note record.
      const synthItem = {
        skuId:            firstSku ? firstSku.skuId : defaultSkuId,
        vendorPartNumber: firstSku ? firstSku.vendorPartNumber : 'CREDIT-NOTE',
        description:      `[CREDIT NOTE] Net refund. Items: ${itemSummary}`.slice(0, 500),
        quantity:         -1,
        cost:             totalAbsRnd
      };

      const combinedBody = baseBody({
        number:     poNumber || undefined,
        requiredOn: poRequiredOn,
        tax:        0,             // tax already folded into cost so ST math = -1 × cost
        shipping:   0,
        memo:       `[CREDIT NOTE] ${memoBase}${origTotalStr} — single-line credit. Negative qty represents the refund.`,
        items:      [synthItem]
      });

      console.log(`[create-po] credit note synthetic line | totalAbs=${totalAbsRnd} | trying qty=-1 first | items in summary: ${itemSummary}`);

      // First try with qty=-1 (negative line total). If ST rejects (likely),
      // fall back to qty=+1 so the post still succeeds and the receipt is at
      // least visible in ST as a positive credit-note record.
      let poData;
      let negQtyAccepted = false;
      try {
        poData = await postToST(`/inventory/v2/tenant/${creds.tenantId}/purchase-orders`, combinedBody, 'create-po');
        negQtyAccepted = true;
        console.log(`[create-po] negative qty ACCEPTED by ST — PO id=${poData.id} should show as -$${totalAbsRnd}`);
      } catch (negErr) {
        console.warn(`[create-po] negative qty rejected by ST (${negErr.message}) — retrying with qty=+1 and positive total`);
        synthItem.quantity = 1;
        combinedBody.memo = `[CREDIT NOTE] ${memoBase}${origTotalStr} — recorded as positive-total PO (ST rejected negative qty). Reconcile manually as a return.`;
        poData = await postToST(`/inventory/v2/tenant/${creds.tenantId}/purchase-orders`, combinedBody, 'create-po');
        console.log(`[create-po] created credit-note PO id=${poData.id} (positive fallback)`);
      }

      return {
        isCredit: true,
        poId: poData.id,
        poNumber: poData.number,
        returnReceiptId: null,
        returnReceiptNumber: null,
        warning: negQtyAccepted
          ? `Posted with negative quantity — ST PO total will display as -$${totalAbsRnd}, matching the source credit note.`
          : `ST rejected negative quantity; posted as positive-total PO ($${totalAbsRnd}). Configure ST_RETURN_TYPE_ID for proper Return Receipt accounting.`
      };
    }

    // ── SPLIT PATH: Return Type available, do it properly ──
    let poResult = null;
    let returnResult = null;

    // 1. Charges → Purchase Order
    if (positiveLines.length > 0) {
      const poItems = await Promise.all(positiveLines.map(li => buildSTItem(li)));
      const poBody = baseBody({
        number:     poNumber || undefined,
        requiredOn: poRequiredOn,
        tax:        0,                       // tax fully allocated to Return Receipt
        shipping:   absNum(shipping),
        memo:       `[CREDIT NOTE — CHARGE PORTION] ${memoBase}${origTotalStr}`,
        items:      poItems
      });
      console.log(`[create-po] CREDIT NOTE charge portion | items=${poItems.length}`);
      poItems.forEach((it, i) => console.log(`[create-po]   item[${i}] sku=${it.skuId} vpn=${it.vendorPartNumber} qty=${it.quantity} cost=${it.cost}`));
      const poData = await postToST(`/inventory/v2/tenant/${creds.tenantId}/purchase-orders`, poBody, 'create-po');
      console.log(`[create-po] created PO id=${poData.id} number=${poData.number}`);
      poResult = { poId: poData.id, poNumber: poData.number };
    }

    // 2. Returns → Return Receipt
    if (negativeLines.length > 0) {
      const rrItems = await Promise.all(negativeLines.map(li => buildSTItem(li, { descPrefix: '[RETURN]' })));

      // Return Receipt body — uses the base scaffold MINUS the PO-only
      // fields (typeId, requiredOn) and PLUS the return-specific ones.
      // We rebuild rather than reuse baseBody() to keep the shapes clean.
      const rrBody = {
        request:             null,            // no Return Request reference (standalone return)
        returnDate:          poDate,
        returnTypeId,
        restockingFee,
        number:              vendorInvoiceNo || poNumber || undefined,
        date:                poDate,
        vendorId:            vendorId || undefined,
        businessUnitId,
        inventoryLocationId,
        shipTo,
        tax:                 absNum(tax),     // entire tax goes here (it's a refund)
        shipping:            0,
        impactsTechnicianPayroll: false,
        memo:                `[CREDIT NOTE — RETURN PORTION] ${memoBase}${origTotalStr}`,
        items:               rrItems
      };
      if (resolvedJobId) rrBody.jobId = resolvedJobId;

      console.log(`[create-rr] CREDIT NOTE return portion | items=${rrItems.length} | tax=${rrBody.tax} | returnTypeId=${returnTypeId} | restockingFee=${restockingFee}`);
      rrItems.forEach((it, i) => console.log(`[create-rr]   item[${i}] sku=${it.skuId} vpn=${it.vendorPartNumber} qty=${it.quantity} cost=${it.cost}`));

      // ST's Return Receipt endpoint. The path can be overridden via
      // ST_RETURN_RECEIPT_PATH in case ST renamed it in your tenant.
      const rrPath = (process.env.ST_RETURN_RECEIPT_PATH || '/inventory/v2/tenant/{tid}/returns').replace('{tid}', creds.tenantId);
      try {
        const rrData = await postToST(rrPath, rrBody, 'create-rr');
        console.log(`[create-rr] created Return Receipt id=${rrData.id} number=${rrData.number}`);
        returnResult = { returnReceiptId: rrData.id, returnReceiptNumber: rrData.number };
      } catch (rrErr) {
        // If the endpoint or schema is wrong for this tenant, surface a clear
        // error so the user knows exactly what to override.
        throw new Error(
          `Return Receipt post failed: ${rrErr.message}. ` +
          `If your ST tenant uses a different default return type, set ` +
          `ST_RETURN_TYPE_ID in .env (current: ${returnTypeId}). ` +
          `If the endpoint path is wrong, set ST_RETURN_RECEIPT_PATH ` +
          `(current: ${rrPath}).`
        );
      }
    }

    return {
      isCredit: true,
      poId: poResult?.poId || null,
      poNumber: poResult?.poNumber || null,
      returnReceiptId: returnResult?.returnReceiptId || null,
      returnReceiptNumber: returnResult?.returnReceiptNumber || null
    };
  }

  // ── Regular invoice path ──
  const shippedItems = (lineItems || []).filter(li => {
    const t = parseFloat(li.total);
    if (Number.isFinite(t) && t === 0) return false;
    if (Number.isFinite(t) && t !== 0) return true;
    const q = parseFloat(li.qty) || 0;
    const u = parseFloat(li.unit) || parseFloat(li.cost) || 0;
    return q > 0 && u !== 0;
  });

  const rawItems = (shippedItems.length > 0)
    ? shippedItems
    : [{ desc: vendorInvoiceNo || vendor || 'Receipt', qty: 1, unit: '0.00', vendorPartNo: '' }];

  const items = await Promise.all(rawItems.map(li => buildSTItem(li)));

  const memo = vendorInvoiceNo ? `Vendor Invoice: ${vendorInvoiceNo}` : undefined;

  const poBody = baseBody({
    number:     poNumber || undefined,
    requiredOn: poRequiredOn,
    tax:        parseFloat(tax)      || 0,
    shipping:   parseFloat(shipping) || 0,
    memo,
    items
  });

  console.log(`[create-po] standard | vendor=${vendor} | jobId=${resolvedJobId || 'none'} | items=${items.length} | tax=${poBody.tax} | shipping=${poBody.shipping}`);
  items.forEach((it, i) => console.log(`[create-po]   item[${i}] sku=${it.skuId} vpn=${it.vendorPartNumber} qty=${it.quantity} cost=${it.cost}`));

  const poData = await postToST(`/inventory/v2/tenant/${creds.tenantId}/purchase-orders`, poBody, 'create-po');
  console.log(`[create-po] created PO id=${poData.id} number=${poData.number}`);
  return { poId: poData.id, poNumber: poData.number };
}

// ── ServiceTitan connectivity test (GET /api/test-st) ──
// Debug endpoint: probes ST for Return Types (used by credit-note posts).
// Visit http://<host>:3002/api/test-st-returns to see what return types
// exist in your tenant — useful when "Return Type with id X doesn't exists!"
// errors come back from /returns.
app.get('/api/test-st-returns', async (req, res) => {
  try {
    const creds = getSTCreds();
    if (!creds) return res.status(503).json({ error: 'ST credentials not configured' });
    const token = await getSTToken(creds);
    const h = { 'Authorization': 'Bearer ' + token, 'ST-App-Key': creds.appKey };
    const base = 'https://api.servicetitan.io';
    const tid  = creds.tenantId;
    const tryPaths = [
      `${base}/inventory/v2/tenant/${tid}/return-types?pageSize=50`,
      `${base}/inventory/v2/tenant/${tid}/returns/types?pageSize=50`,
      `${base}/inventory/v2/tenant/${tid}/return-receipt-types?pageSize=50`,
      `${base}/inventory/v2/tenant/${tid}/inventory-return-types?pageSize=50`,
      `${base}/inventory/v2/tenant/${tid}/vendor-return-types?pageSize=50`,
      `${base}/inventory/v2/tenant/${tid}/material-return-types?pageSize=50`,
      `${base}/inventory/v2/tenant/${tid}/return-reasons?pageSize=50`,
      `${base}/inventory/v2/tenant/${tid}/returns?pageSize=1`,
      `${base}/inventory/v2/tenant/${tid}/types/return?pageSize=50`,
      `${base}/inventory/v2/tenant/${tid}/types?pageSize=50`,
      `${base}/settings/v2/tenant/${tid}/return-types?pageSize=50`,
      `${base}/settings/v2/tenant/${tid}/inventory/return-types?pageSize=50`
    ];
    const probes = {};
    for (const url of tryPaths) {
      try {
        const r = await fetch(url, { headers: h });
        const body = await r.text();
        let parsed; try { parsed = JSON.parse(body); } catch { parsed = body.slice(0, 400); }
        probes[url] = { status: r.status, body: parsed };
      } catch (e) {
        probes[url] = { error: e.message };
      }
    }
    return res.json({ ok: true, hint: 'Set ST_RETURN_TYPE_ID in .env to a valid id from one of these responses.', probes });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

app.get('/api/test-st', async (req, res) => {
  const result = {};
  try {
    const creds = getSTCreds();
    if (!creds) return res.json({ ok: false, step: 'creds', error: 'Missing ST env vars', result });
    result.creds = {
      tenantId: creds.tenantId,
      clientId: creds.clientId.slice(0, 12) + '...',
      hasSecret: !!creds.clientSecret,
      appKey: creds.appKey.slice(0, 10) + '...'
    };

    const token = await getSTToken(creds);
    result.token = 'ok (length=' + token.length + ')';

    const h = { 'Authorization': 'Bearer ' + token, 'ST-App-Key': creds.appKey };
    const base = 'https://api.servicetitan.io';
    const tid = creds.tenantId;

    async function probe(url) {
      try {
        const r = await fetch(url, { headers: h });
        const d = await r.json();
        const items = (d.data || d.items || []).slice(0, 5).map(x => ({ id: x.id, name: x.name || x.label || x.number }));
        return { status: r.status, items, totalCount: d.totalCount };
      } catch (e) { return { error: e.message }; }
    }

    const [vendors, poTypes, bu1, bu2, bu3, loc1, loc2, loc3] = await Promise.all([
      probe(`${base}/inventory/v2/tenant/${tid}/vendors?pageSize=5`),
      probe(`${base}/inventory/v2/tenant/${tid}/purchase-order-types?active=true&pageSize=5`),
      probe(`${base}/businessunits/v2/tenant/${tid}/business-units?active=true&pageSize=5`),
      probe(`${base}/settings/v2/tenant/${tid}/business-units?active=true&pageSize=5`),
      probe(`${base}/tenant/v2/tenant/${tid}/business-units?active=true&pageSize=5`),
      probe(`${base}/inventory/v2/tenant/${tid}/trucks?active=true&pageSize=5`),
      probe(`${base}/inventory/v2/tenant/${tid}/inventory-locations?active=true&pageSize=5`),
      probe(`${base}/inventory/v2/tenant/${tid}/locations?active=true&pageSize=5`)
    ]);

    result.vendors = vendors;
    result.poTypes = poTypes;
    result.businessUnits = { 'businessunits/v2': bu1, 'settings/v2': bu2, 'tenant/v2': bu3 };
    result.inventoryLocations = { 'trucks': loc1, 'inventory-locations': loc2, 'locations': loc3 };
    result.envOverrides = {
      ST_TYPE_ID:         process.env.ST_TYPE_ID     || '(auto)',
      ST_BU_ID:           process.env.ST_BU_ID       || '(auto)',
      ST_LOCATION_ID:     process.env.ST_LOCATION_ID || '(auto)',
      ST_DEFAULT_SKU_ID:  process.env.ST_DEFAULT_SKU_ID || '0',
      ST_SHIP_TO:         process.env.ST_SHIP_TO     || 'Main Location'
    };

    return res.json({ ok: true, step: 'done', result });
  } catch (err) {
    return res.json({ ok: false, step: 'error', error: err.message, result });
  }
});

// ── ServiceTitan: list all vendors (debug) ──
app.get('/api/st-vendors', async (req, res) => {
  try {
    const creds = getSTCreds();
    if (!creds) return res.status(503).json({ error: 'No ST credentials' });
    const token = await getSTToken(creds);
    const { availableVendors } = await lookupSTVendor(token, creds, null);
    return res.json({ count: availableVendors.length, vendors: availableVendors });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

// ── ServiceTitan: create purchase order (manual UI post) ──
app.post('/api/create-po', async (req, res) => {
  try {
    const creds = getSTCreds();
    if (!creds) return res.status(503).json({ error: 'ServiceTitan credentials not configured' });

    const { poNumber, vendor, vendorInvoiceNo, date, requiredDate, tax, shipping, jobId, lineItems, total } = req.body || {};

    const blocker = validateForServiceTitan({ vendor, jobId, lineItems, total, tax });
    if (blocker) return res.status(400).json({ error: blocker });

    const result = await createSTPurchaseOrder({ poNumber, vendor, vendorInvoiceNo, date, requiredDate, tax, shipping, jobId, lineItems, total });
    return res.json({ success: true, ...result });
  } catch (err) {
    console.error('[create-po] error:', err.message, err.stack);
    return res.status(500).json({ error: err.message || String(err) });
  }
});

// ── Queue a manually uploaded file for background processing ──
app.post('/api/queue-upload', async (req, res) => {
  try {
    const { fileName, fileUrl, fileType } = req.body || {};
    if (!fileName || !fileUrl || !fileType) {
      return res.status(400).json({ error: 'Missing fileName, fileUrl, or fileType' });
    }
    const sb = await getSupabaseAdmin();
    const { data, error } = await sb.from('upload_queue').insert({
      user_id: req.user.id,
      file_name: fileName,
      file_url: fileUrl,
      file_type: fileType,
      status: 'pending'
    }).select('id').single();
    if (error) return res.status(500).json({ error: error.message });

    // Fire Inngest event — triggers background processing immediately
    await inngest.send({ name: 'receipt/queued', data: { queueId: data.id } });

    return res.json({ success: true, id: data.id });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

// ── Create Jobber expense ──
app.post('/api/create-expense', async (req, res) => {
  try {
    const { vendor, invoiceNo, date, total, jobNo, receiptBlobUrl } = req.body;

    if (!jobNo) {
      return res.status(400).json({
        error: 'No job number found. Please enter one before posting to Jobber.'
      });
    }

    const num = parseInt(jobNo, 10);
    if (isNaN(num)) {
      return res.status(400).json({ error: `"${jobNo}" is not a valid job number.` });
    }

    const numStr = String(num);

    let job = null;
    let lastError = null;

    // Helper: extract nodes from either `nodes` or `edges { node }` pattern
    const extractNodes = (jobsObj) => {
      if (!jobsObj) return [];
      if (Array.isArray(jobsObj.nodes) && jobsObj.nodes.length > 0) return jobsObj.nodes;
      if (Array.isArray(jobsObj.edges) && jobsObj.edges.length > 0) return jobsObj.edges.map(e => e.node).filter(Boolean);
      if (Array.isArray(jobsObj.nodes)) return jobsObj.nodes; // empty nodes array is still valid
      return [];
    };

    // Strategy 1: searchTerm
    for (const term of [numStr, `#${numStr}`]) {
      const result = await jobberGQL(`
        query FindJob($term: String!) {
          jobs(first: 100, searchTerm: $term) {
            nodes { id jobNumber title }
            edges { node { id jobNumber title } }
          }
        }
      `, { term });

      console.log(`[jobber] searchTerm="${term}" keys:`, Object.keys(result.data?.jobs || {}), 'errors:', result.errors?.length || 0);

      if (result.errors?.length) {
        const msg = result.errors[0].message || '';
        if (/unauthori|token|auth/i.test(msg)) {
          return res.status(401).json({ error: 'Jobber session expired. Go to Settings → Authorize Jobber to reconnect.' });
        }
        lastError = msg;
        continue;
      }

      const nodes = extractNodes(result.data?.jobs);
      console.log(`[jobber] searchTerm="${term}" returned ${nodes.length} jobs:`, nodes.map(j => j.jobNumber));
      job = nodes.find(j => String(j.jobNumber) === numStr);
      if (job) break;
    }

    // Strategy 2: paginate all jobs — supports both nodes and edges patterns
    if (!job) {
      console.log(`[jobber] searchTerm missed #${numStr}, paginating all jobs...`);
      let cursor = null;
      for (let page = 0; page < 20 && !job; page++) {
        const query = cursor
          ? `query PageJobs($cursor: String!) {
              jobs(first: 100, after: $cursor) {
                nodes { id jobNumber title }
                edges { node { id jobNumber title } }
                pageInfo { hasNextPage endCursor }
              }
            }`
          : `query PageJobs {
              jobs(first: 100) {
                nodes { id jobNumber title }
                edges { node { id jobNumber title } }
                pageInfo { hasNextPage endCursor }
              }
            }`;

        const result = await jobberGQL(query, cursor ? { cursor } : {});

        if (result.errors?.length) {
          console.log(`[jobber] page ${page + 1} error:`, result.errors[0].message);
          lastError = result.errors[0].message || lastError;
          break;
        }

        const jobsObj = result.data?.jobs;
        const nodes = extractNodes(jobsObj);
        const pageInfo = jobsObj?.pageInfo || {};

        if (page === 0) console.log(`[jobber] page 1 raw:`, JSON.stringify(result).substring(0, 800));
        console.log(`[jobber] page ${page + 1}: ${nodes.length} jobs, hasNext=${pageInfo.hasNextPage}, keys=${Object.keys(jobsObj || {})}`);

        job = nodes.find(j => String(j.jobNumber) === numStr);
        if (job || !pageInfo.hasNextPage) break;
        cursor = pageInfo.endCursor;
      }
    }

    if (lastError && !job) {
      return res.status(400).json({ error: 'Jobber API error: ' + lastError });
    }

    if (!job) {
      return res.status(404).json({
        error: `Job #${num} not found in Jobber. Check the job number and try again.`
      });
    }

    const receiptNote = receiptBlobUrl ? 'attached' : null;
    const titleParts = [vendor, invoiceNo ? `Invoice #${invoiceNo}` : null].filter(Boolean);
    const expenseTitle = titleParts.length ? titleParts.join(' — ') : 'Expense';

    const parsedTotal = parseFloat(total);
    const expenseTotal = isNaN(parsedTotal) ? 0 : parsedTotal;

    const expInput = {
      linkedJobId: job.id,
      title: expenseTitle,
      total: expenseTotal,
      date: (date || new Date().toISOString().split('T')[0]) + 'T00:00:00Z'
    };

    if (invoiceNo) expInput.description = `Invoice #${invoiceNo}`;
    if (receiptBlobUrl) expInput.receiptUrl = receiptBlobUrl;

    const expResult = await jobberGQL(`
      mutation CreateExpense($input: ExpenseCreateInput!) {
        expenseCreate(input: $input) {
          expense { id title total }
          userErrors { message path }
        }
      }
    `, { input: expInput });

    console.log('Jobber expense create response:', JSON.stringify(expResult));

    const errors = expResult.data?.expenseCreate?.userErrors;
    if (errors?.length) {
      return res.status(400).json({ error: errors[0].message, raw: expResult });
    }

    const expense = expResult.data?.expenseCreate?.expense;
    if (!expense?.id) {
      return res.status(500).json({
        error: 'Jobber accepted the request but returned no expense. The "Expenses" write scope may not be enabled on your Jobber app.',
        raw: expResult
      });
    }

    return res.json({
      success: true,
      expenseId: expense.id,
      jobTitle: job.title,
      receiptNote
    });
  } catch (err) {
    if (err.message === 'NOT_CONNECTED') {
      return res.status(401).json({
        error: 'Not connected to Jobber. Go to Settings to connect.'
      });
    }

    return res.status(500).json({ error: err.message });
  }
});


// ── Queue a manual file upload for background ST processing ──
app.post('/api/queue-manual-upload', upload.single('receipt'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });

    const { buffer: fileBuffer, mimetype: mimeType, originalname: fileName } = req.file;
    const ext = mimeType === 'application/pdf' ? 'pdf' : (mimeType.split('/')[1] || 'jpg');
    const storagePath = `uploads/${Date.now()}_${fileName.replace(/[^a-zA-Z0-9._-]/g, '_')}.${ext}`;

    const sb = await getSupabaseAdmin();

    // Upload to Supabase Storage
    const { error: storageErr } = await sb.storage
      .from('receipts')
      .upload(storagePath, fileBuffer, { contentType: mimeType, upsert: false });

    if (storageErr) return res.status(500).json({ error: 'Storage upload failed: ' + storageErr.message });

    const { data: { publicUrl: fileUrl } } = sb.storage.from('receipts').getPublicUrl(storagePath);

    const userId = (process.env.SYSTEM_USER_ID || '').trim() || null;

    const { data: row, error: insertErr } = await sb.from('incoming_receipts').insert({
      user_id: userId,
      file_url: fileUrl,
      file_name: fileName,
      storage_path: storagePath,
      status: 'pending'
    }).select('id').single();

    if (insertErr) return res.status(500).json({ error: insertErr.message });

    const incoming = { id: row.id, file_url: fileUrl, file_name: fileName, user_id: userId, storage_path: storagePath };
    processIncomingForST(sb, incoming).catch(err =>
      console.error(`[queue-manual-upload] bg error for ${row.id}:`, err.message)
    );

    return res.json({ success: true, id: row.id, fileUrl });
  } catch (err) {
    console.error('[queue-manual-upload] error:', err);
    return res.status(500).json({ error: err.message });
  }
});

// ── Poll status of a manually-queued upload ──
app.get('/api/incoming-status/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const sb = await getSupabaseAdmin();
    const { data: row, error } = await sb.from('incoming_receipts')
      .select('id, status, error, result, processed_at').eq('id', id).single();
    if (error || !row) return res.status(404).json({ error: 'Not found' });
    return res.json({ status: row.status, error: row.error || null, data: row.result || null });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

// ── In-flight manual uploads (pending/processing) — lets frontend resume polling after refresh ──
app.get('/api/incoming-pending', async (req, res) => {
  try {
    const sb = await getSupabaseAdmin();
    const cutoff = new Date(Date.now() - 60 * 60 * 1000).toISOString(); // last hour only
    const { data, error } = await sb.from('incoming_receipts')
      .select('id, file_name, file_url, storage_path, status, created_at')
      .in('status', ['pending', 'processing'])
      .gte('created_at', cutoff)
      .order('created_at', { ascending: false });
    if (error) return res.status(500).json({ error: error.message });
    return res.json({ items: data || [] });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

// ── List all receipts from DB ──
app.get('/api/receipts-list', async (req, res) => {
  try {
    const sb = await getSupabaseAdmin();
    const { data, error } = await sb.from('receipts')
      .select('*')
      .order('saved_at', { ascending: false })
      .limit(200);
    if (error) return res.status(500).json({ error: error.message });
    return res.json({ receipts: data || [] });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

// ── History from incoming_receipts (manual uploads) ──
app.get('/api/incoming-history', async (req, res) => {
  try {
    const sb = await getSupabaseAdmin();
    const { data, error } = await sb.from('incoming_receipts')
      .select('id, file_name, storage_path, file_url, status, error, result, processed_at, created_at')
      .eq('status', 'done')
      .order('processed_at', { ascending: false })
      .limit(200);
    if (error) return res.status(500).json({ error: error.message });
    return res.json({ receipts: data || [] });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

// ── Update receipt status in DB ──
app.patch('/api/receipt-status', async (req, res) => {
  try {
    const { id, status, stPurchaseOrderId } = req.body || {};
    if (!id || !status) return res.status(400).json({ error: 'Missing id or status' });
    const sb = await getSupabaseAdmin();
    const update = { status };
    if (stPurchaseOrderId) update.st_purchase_order_id = stPurchaseOrderId;
    const { error } = await sb.from('receipts').update(update).eq('id', id);
    if (error) return res.status(500).json({ error: error.message });
    return res.json({ success: true });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

// ── Delete a receipt from DB ──
app.delete('/api/receipt/:id', async (req, res) => {
  try {
    const { id } = req.params;
    if (!id) return res.status(400).json({ error: 'Missing id' });
    const sb = await getSupabaseAdmin();
    // Try deleting from receipts table
    await sb.from('receipts').delete().eq('id', id);
    // Also try upload_queue (Gmail receipts live there)
    await sb.from('upload_queue').delete().eq('id', id);
    // Also try incoming_receipts
    await sb.from('incoming_receipts').delete().eq('id', id);
    return res.json({ success: true });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

// ── Gmail receipts — all statuses for the Gmail Receipts page ──
app.get('/api/gmail-receipts', async (req, res) => {
  try {
    const sb = await getSupabaseAdmin();
    // incoming_receipts = email-sourced receipts; return all statuses so the page shows live progress
    const { data: incoming, error: e1 } = await sb.from('incoming_receipts')
      .select('*')
      .order('created_at', { ascending: false })
      .limit(200);
    if (e1) return res.status(500).json({ error: e1.message });
    return res.json({ receipts: incoming || [] });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

// Serve index.html for all non-API routes (VPS mode)
const path = require('path');
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

module.exports = app;

async function cleanupStuckRows() {
  try {
    const sb = await getSupabaseAdmin();
    const cutoff = new Date(Date.now() - 10 * 60 * 1000).toISOString(); // older than 10 min
    const { data, error } = await sb.from('incoming_receipts')
      .update({ status: 'failed', error: 'Processing interrupted — server restarted. Please re-upload.' })
      .in('status', ['pending', 'processing'])
      .lt('created_at', cutoff)
      .select('id');
    if (!error && data?.length) {
      console.log(`[startup] marked ${data.length} stuck incoming_receipts row(s) as failed`);
    }
  } catch (err) {
    console.error('[startup] cleanupStuckRows error:', err.message);
  }
}

if (require.main === module) {
  app.listen(PORT, () => {
    console.log(`ReceiptFlow server running at http://localhost:${PORT}`);
    cleanupStuckRows();
  });
}

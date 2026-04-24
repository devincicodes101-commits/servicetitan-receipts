require('dotenv').config();
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

    const geminiOutput = await parseWithGemini(fileBuffer, mimeType);
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

// ── n8n endpoint: POST { rowId } → Vercel downloads file directly from Supabase ──
app.post('/api/process-incoming-row', async (req, res) => {
  const secret = (process.env.PROCESS_SECRET || '').trim();
  const provided = (req.headers['x-process-secret'] || '').trim();
  if (secret && provided !== secret) return res.status(401).json({ error: 'Unauthorized' });

  const rowId = (req.body || {}).rowId;
  if (!rowId) return res.status(400).json({ error: 'Missing rowId' });

  const sb = await getSupabaseAdmin();
  const { data: row } = await sb.from('incoming_receipts').select('*').eq('id', rowId).single();
  if (!row) return res.status(404).json({ error: 'Row not found' });
  if (row.status !== 'pending') return res.json({ success: true, message: `Row already ${row.status}` });

  const { data: claimed } = await sb.from('incoming_receipts')
    .update({ status: 'processing' })
    .eq('id', rowId).eq('status', 'pending').select('id');

  if (!claimed || claimed.length === 0) return res.json({ success: true, message: 'Already claimed' });

  const result = await processOneRow(sb, row);
  return res.json(result);
});

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

    const geminiOutput = await parseWithGemini(fileBuffer, mimeType);
    const fields = extractFieldsFromLlama(geminiOutput);
    console.log(`[process-queue] ${rowId}: fields=`, JSON.stringify(fields));

    await sb.from('upload_queue').update({
      status: 'done',
      vendor: fields.vendor || null,
      invoice_no: fields.invoiceNo || null,
      date: fields.date || null,
      amount: parseFloat(fields.total) || 0,
      job_no: fields.jobNo || null,
      items: fields.items || [],
      error: null,
      processed_at: new Date().toISOString()
    }).eq('id', rowId);

    return { id: rowId, success: true };
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
  if (req.path === '/api/process-incoming' || req.path === '/api/process-queue' || req.path === '/api/process-incoming-row') return next();
  if (req.path.startsWith('/api/inngest')) return next();
  const open = [
    '/api/config',
    '/api/health',
    '/api/auth/jobber',
    '/api/auth/callback',
    '/api/jobber-status',
    '/api/jobber-debug'
  ];

  if (!req.path.startsWith('/api/') || open.includes(req.path)) return next();

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
  const items = [];

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

    let m = str.match(/(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})/);
    if (m) return `${m[3]}-${m[1].padStart(2, '0')}-${m[2].padStart(2, '0')}`;

    m = str.match(/(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})/);
    if (m) return `${m[1]}-${m[2].padStart(2, '0')}-${m[3].padStart(2, '0')}`;

    return null;
  }

  const tables = parseTables(content);
  const lmap = {};

  // Labels where we want the largest monetary value (grand total > line total)
  const MONETARY_LABELS = new Set([
    'TOTAL', 'GRAND TOTAL', 'INVOICE TOTAL', 'AMOUNT DUE',
    'BALANCE DUE', 'TOTAL DUE', 'SUBTOTAL', 'GROSS TOTAL'
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

  vendor =
    headingMatch ||
    (boldMatch?.[1]?.trim() && !DOC_KEYWORDS.test(boldMatch[1]) ? boldMatch[1].trim() : null) ||
    lmap['VENDOR'] ||
    lmap['SUPPLIER'] ||
    lmap['COMPANY'] ||
    lmap['SOLD BY'] ||
    lmap['BILLED BY'] ||
    null;

  invoiceNo =
    lmap['ORDER NO'] ||
    lmap['INVOICE NO'] ||
    lmap['INVOICE NUMBER'] ||
    lmap['INVOICE #'] ||
    lmap['ORDER NUMBER'] ||
    lmap['DOCUMENT NO'] ||
    lmap['RECEIPT NO'] ||
    lmap['RECEIPT NUMBER'] ||
    lmap['TRANSACTION NO'] ||
    null;

  const rawDate =
    lmap['INVOICE DATE'] ||
    lmap['DATE'] ||
    lmap['TRANSACTION DATE'] ||
    lmap['BILL DATE'] ||
    lmap['SALE DATE'] ||
    lmap['RECEIPT DATE'] ||
    lmap['ISSUED'] ||
    lmap['ORDER DATE'] ||
    null;

  date = parseDate(rawDate);
  if (!date) {
    const anyDate = content.match(/\b(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{4})\b/);
    if (anyDate) date = parseDate(anyDate[1]);
  }

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

    let m = val.match(/^(\d{3,7})$/);
    if (m) {
      jobNo = m[1];
      break;
    }

    // Handle "1095 - RETURN", "1391-CREDIT" etc. with optional spaces around dash
    m = val.match(/^(\d{3,7})\s*[-–]\s*[A-Z]/i);
    if (m) {
      jobNo = m[1];
      break;
    }

    m = val.match(/^(\d{3,7})[-\s]/);
    if (m) {
      jobNo = m[1];
      break;
    }
  }

  // Fallback: scan raw text for P.O. / PO number patterns
  // Handles "PO Number\n1180", "YOUR P.O. NO 1456", "PO Number | 1180" etc.
  if (!jobNo) {
    const poMatch =
      content.match(/\bP\.?\s*O\.?\s*(?:Number|No|NO|#|NUM|NUMBER)\b[^\d]{0,120}?(\d{3,7})\b/i) ||
      content.match(/\bYour\s+P\.?\s*O\.?\s*(?:No|NO|Number)\b[^\d]{0,120}?(\d{3,7})\b/i) ||
      content.match(/\bPurchase\s*Order\b[^\d]{0,120}?(\d{3,7})\b/i);
    if (poMatch) jobNo = poMatch[1];
  }

  // Fallback: scan raw text for NNNN-RETURN / NNNN-CREDIT patterns
  if (!jobNo) {
    const returnMatch = content.match(/\b(\d{3,7})[-–]\s*(?:RETURN|CREDIT|RMA|VOID)\b/i);
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

  console.log('[fields] extracted:', { vendor, invoiceNo, date, jobNo, total, itemCount: items.length });
  return { vendor, invoiceNo, date, jobNo, total, items };
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
async function parseWithGemini(fileBuffer, mimeType) {
  const ai = await getGeminiClient();

  const prompt = `You are a receipt and invoice parser. Extract ALL content from this document exactly as printed.

Formatting rules (follow exactly):
- Output the vendor/company name as a # H1 heading (e.g. "# GESCAN")
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
- If the document is labelled "CREDIT", "RETURN", or "CREDIT - DO NOT PAY", verify all totals have trailing minus signs and output them as negative.`;

  if (mimeType === 'application/pdf') {
    // Upload via Files API so Gemini processes every page of the PDF
    const blob = new Blob([fileBuffer], { type: mimeType });
    const uploadedFile = await ai.files.upload({
      file: blob,
      config: { mimeType, displayName: 'receipt.pdf' }
    });

    console.log('[gemini] uploaded PDF for multi-page processing, uri:', uploadedFile.uri);

    const response = await ai.models.generateContent({
      model: GEMINI_MODEL,
      contents: [
        { text: prompt },
        { fileData: { mimeType, fileUri: uploadedFile.uri } }
      ]
    });

    const text = response.text || '';
    if (!text) throw new Error('Gemini returned empty response');

    console.log('[gemini] output preview:', text.substring(0, 300));
    return text;
  }

  // For images, use inlineData directly
  const response = await ai.models.generateContent({
    model: GEMINI_MODEL,
    contents: [
      { text: prompt },
      {
        inlineData: {
          mimeType,
          data: fileBuffer.toString('base64')
        }
      }
    ]
  });

  const text = response.text || '';
  if (!text) throw new Error('Gemini returned empty response');

  console.log('[gemini] output preview:', text.substring(0, 300));
  return text;
}

app.post('/api/extract', upload.single('receipt'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    const fileBuffer = req.file.buffer;
    const mimeType = req.file.mimetype;

    console.log('[extract] mimetype:', mimeType, '| size:', fileBuffer.length, '| file:', req.file.originalname);

    const geminiOutput = await parseWithGemini(fileBuffer, mimeType);
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

    const geminiOutput = await parseWithGemini(fileBuffer, mType);
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


// Serve index.html for all non-API routes (VPS mode)
const path = require('path');
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

module.exports = app;

if (require.main === module) {
  app.listen(PORT, () => {
    console.log(`ReceiptFlow server running at http://localhost:${PORT}`);
  });
}

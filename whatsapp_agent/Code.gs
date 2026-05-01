const SPREADSHEET_ID = 'REPLACE_WITH_SPREADSHEET_ID';
const TZ = Session.getScriptTimeZone() || 'Asia/Kolkata';

function doGet() {
  return HtmlService.createTemplateFromFile('Dashboard')
    .evaluate()
    .setTitle('WhatsApp Send-Ready Dashboard')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

function include(filename) {
  return HtmlService.createHtmlOutputFromFile(filename).getContent();
}

function createDailyTrigger() {
  ScriptApp.getProjectTriggers()
    .filter(t => t.getHandlerFunction() === 'prepareDailyQueue')
    .forEach(t => ScriptApp.deleteTrigger(t));

  ScriptApp.newTrigger('prepareDailyQueue')
    .timeBased()
    .everyDays(1)
    .atHour(9)
    .create();
}

function prepareDailyQueue() {
  const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
  const contacts = getSheetRows_(ss, 'contacts_events');
  const templates = getSheetRows_(ss, 'message_templates');
  const today = Utilities.formatDate(new Date(), TZ, 'yyyy-MM-dd');

  const todays = contacts.filter(r =>
    String(r.event_date || '') === today &&
    String(r.active || '').toUpperCase() === 'TRUE'
  );

  const output = todays.map(r => buildQueueRecord_(r, templates, today));
  writeReadyQueue_(ss, output);
}

function getTodayQueue() {
  const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
  const queue = getSheetRows_(ss, 'ready_queue');
  const today = Utilities.formatDate(new Date(), TZ, 'yyyy-MM-dd');
  return queue.filter(r => String(r.queue_date) === today);
}

function markAction(id, action) {
  const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
  const sh = ss.getSheetByName('ready_queue');
  const data = sh.getDataRange().getValues();
  const headers = data[0];
  const idCol = headers.indexOf('id');
  const actionCol = headers.indexOf('action_status');
  const tsCol = headers.indexOf('action_ts');

  for (let i = 1; i < data.length; i++) {
    if (String(data[i][idCol]) === String(id)) {
      sh.getRange(i + 1, actionCol + 1).setValue(action);
      sh.getRange(i + 1, tsCol + 1).setValue(new Date());
      return { ok: true };
    }
  }
  return { ok: false };
}

function buildQueueRecord_(row, templates, dateStr) {
  const text = renderTemplate_(row, templates);
  const media = buildMedia_(row, text);
  const waLink = buildWhatsappLink_(row, text, media);

  return {
    id: row.id,
    queue_date: dateStr,
    name: row.name,
    chat_type: row.chat_type,
    event_type: row.event_type,
    phone: row.phone || '',
    group_invite_link: row.group_invite_link || '',
    media_mode: row.media_mode,
    media_url: media.media_url,
    final_message_text: text,
    wa_link: waLink,
    action_status: 'ready',
    action_ts: ''
  };
}

function renderTemplate_(row, templates) {
  const exact = templates.find(t =>
    t.event_type === row.event_type &&
    t.language === row.language &&
    t.tone === row.tone
  );

  const fallback = templates.find(t =>
    t.event_type === row.event_type && t.language === row.language
  ) || templates.find(t => t.event_type === row.event_type)
    || { template_text: 'Hi {{name}}, wishing you a wonderful day!' };

  const template = (exact || fallback).template_text;
  return String(template).replace(/\{\{name\}\}/g, row.name || 'there');
}

function buildMedia_(row, text) {
  if (row.media_mode === 'manual_photo' || row.media_mode === 'text') {
    return { media_url: '' };
  }

  const festival = encodeURIComponent(String(row.event_type || 'celebration'));
  const name = encodeURIComponent(String(row.name || 'friend'));
  const caption = encodeURIComponent(text.slice(0, 80));

  if (row.media_mode === 'gif') {
    return {
      media_url: `https://dummyimage.com/600x600/000/fff.gif&text=${festival}+${name}`
    };
  }

  return {
    media_url: `https://dummyimage.com/1080x1080/ff6600/ffffff.png&text=${festival}+${name}+${caption}`
  };
}

function buildWhatsappLink_(row, text, media) {
  const encodedText = encodeURIComponent(text + (media.media_url ? `\n${media.media_url}` : ''));

  if (row.chat_type === 'individual' && row.phone) {
    return `https://wa.me/${String(row.phone).replace(/\D/g, '')}?text=${encodedText}`;
  }

  if (row.chat_type === 'group' && row.group_invite_link) {
    return row.group_invite_link;
  }

  return '';
}

function getSheetRows_(ss, sheetName) {
  const sh = ss.getSheetByName(sheetName);
  if (!sh) throw new Error(`Missing sheet: ${sheetName}`);
  const values = sh.getDataRange().getValues();
  if (values.length < 2) return [];
  const headers = values[0].map(String);

  return values.slice(1).map(r => {
    const obj = {};
    headers.forEach((h, i) => obj[h] = r[i]);
    return obj;
  });
}

function writeReadyQueue_(ss, rows) {
  let sh = ss.getSheetByName('ready_queue');
  if (!sh) sh = ss.insertSheet('ready_queue');

  const headers = [
    'id','queue_date','name','chat_type','event_type','phone','group_invite_link',
    'media_mode','media_url','final_message_text','wa_link','action_status','action_ts'
  ];

  sh.clearContents();
  sh.getRange(1, 1, 1, headers.length).setValues([headers]);

  if (!rows.length) return;
  const matrix = rows.map(r => headers.map(h => r[h]));
  sh.getRange(2, 1, matrix.length, headers.length).setValues(matrix);
}

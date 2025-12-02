/**
 * Hebrew Learn - Express + SQLite
 */
const express = require('express');
const session = require('express-session');
const SQLiteStore = require('connect-sqlite3')(session);
const bcrypt = require('bcrypt');
const methodOverride = require('method-override');
const path = require('path');
const expressLayouts = require('express-ejs-layouts');

const db = require('./db');

// Helpers promisifiÃ©s pour sqlite3
function run(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function (err) {
      if (err) reject(err);
      else resolve(this);
    });
  });
}

function get(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (err, row) => {
      if (err) reject(err);
      else resolve(row);
    });
  });
}

function all(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) reject(err);
      else resolve(rows);
    });
  });
}

async function findFirstAvailableId(table) {
  const rows = await all(`SELECT id FROM ${table} ORDER BY id ASC`);
  let expected = 1;
  for (const row of rows) {
    if (row.id !== expected) return expected;
    expected++;
  }
  return expected;
}

async function createIdAllocator(table) {
  const rows = await all(`SELECT id FROM ${table} ORDER BY id ASC`);
  const used = new Set(rows.map(r => Number(r.id)));
  let next = 1;
  while (used.has(next)) next++;
  return () => {
    const id = next;
    used.add(id);
    do {
      next++;
    } while (used.has(next));
    return id;
  };
}

const MODE_FLASHCARDS = 'flashcards';
const MODE_FLASHCARDS_REVERSE = 'flashcards_reverse';
const MODE_WRITTEN = 'written';
const ALLOWED_MODES = [MODE_FLASHCARDS, MODE_FLASHCARDS_REVERSE, MODE_WRITTEN];
const DEFAULT_TRAIN_PARAMS = {
  modes: [MODE_FLASHCARDS],
  rev_mode: 'order',
  show_phonetic: 1,
  theme_ids: [],
  set_ids: [],
  level_id: '',
  scope: 'all',
  remaining: 'all',
  total: 'all'
};

function normalizeMode(raw) {
  const value = (raw || '').toLowerCase();
  if (value === 'quiz') return MODE_WRITTEN; // compat ancien nom
  if (ALLOWED_MODES.includes(value)) return value;
  return MODE_FLASHCARDS;
}

function normalizeModes(raw) {
  const list = Array.isArray(raw) ? raw : raw ? [raw] : [];
  const normalized = [];
  for (const val of list) {
    const m = normalizeMode(val);
    if (!normalized.includes(m)) normalized.push(m);
  }
  if (normalized.length === 0) normalized.push(MODE_FLASHCARDS);
  return normalized;
}

function pickQuestionMode(modes = []) {
  const list = modes && modes.length ? modes : [MODE_FLASHCARDS];
  return list[Math.floor(Math.random() * list.length)];
}

function modeLabel(mode) {
  switch (mode) {
    case MODE_FLASHCARDS:
      return 'Flashcards classique';
    case MODE_FLASHCARDS_REVERSE:
      return 'Flashcards reverse';
    case MODE_WRITTEN:
      return 'Révision par écrit';
    default:
      return 'Révision';
  }
}

async function fetchNextOrderedWord(userId, filters = {}) {
  const params = [userId, userId, userId, userId];
  const excludedIds = Array.isArray(filters.excludedIds)
    ? filters.excludedIds.map(id => Number(id)).filter(id => !Number.isNaN(id))
    : [];
  let sql = `SELECT w.*,
        IFNULL(p.strength, 0) AS strength,
        p.last_seen,
        p.id AS progress_id,
        fav.id AS fav_id
       FROM words w
       LEFT JOIN progress p ON p.word_id = w.id AND p.user_id = ?
       LEFT JOIN favorites fav ON fav.word_id = w.id AND fav.user_id = ?
       LEFT JOIN user_word_overrides uwo ON uwo.word_id = w.id AND uwo.user_id = ?
       LEFT JOIN themes t ON t.id = w.theme_id
       LEFT JOIN user_theme_overrides uto ON uto.theme_id = w.theme_id AND uto.user_id = ?
       LEFT JOIN theme_levels l ON l.id = w.level_id
       WHERE w.active = 1
         AND COALESCE(uwo.active, 1) = 1
         AND (w.theme_id IS NULL OR (t.active = 1 AND COALESCE(uto.active, 1) = 1))
         AND (w.level_id IS NULL OR l.active = 1)`;

  if (filters.theme_id) {
    sql += ' AND w.theme_id = ?';
    params.push(filters.theme_id);
  } else if (filters.theme_ids && filters.theme_ids.length > 0) {
    const placeholders = filters.theme_ids.map(() => '?').join(',');
    sql += ` AND w.theme_id IN (${placeholders})`;
    params.push(...filters.theme_ids);
  }
  if (filters.level_id) {
    sql += ' AND w.level_id = ?';
    params.push(filters.level_id);
  }
  if (filters.difficulty) {
    sql += ' AND w.difficulty = ?';
    params.push(filters.difficulty);
  }

  if (filters.scope === 'global') {
    sql += ' AND w.user_id IS NULL';
  } else if (filters.scope === 'mine') {
    sql += ' AND w.user_id = ?';
    params.push(userId);
  } else if (filters.scope === 'none') {
    return null;
  } else {
    sql += ' AND (w.user_id IS NULL OR w.user_id = ?)';
    params.push(userId);
  }

  if (filters.global_theme_ids && filters.global_theme_ids.length > 0 && filters.scope !== 'mine') {
    const placeholders = filters.global_theme_ids.map(() => '?').join(',');
    sql += ` AND (w.user_id = ? OR w.theme_id IN (${placeholders}))`;
    params.push(null, ...filters.global_theme_ids);
  }

  if (excludedIds.length > 0) {
    const placeholders = excludedIds.map(() => '?').join(',');
    sql += ` AND w.id NOT IN (${placeholders})`;
    params.push(...excludedIds);
  }

  if (filters.theme_order && filters.theme_order.length > 0) {
    const caseExpr = filters.theme_order.map((id, idx) => `WHEN ? THEN ${idx}`).join(' ');
    sql += ` ORDER BY CASE w.theme_id ${caseExpr} ELSE 2147483647 END ASC, COALESCE(w.position, 2147483647) ASC, w.id ASC LIMIT 1`;
    params.push(...filters.theme_order.map(Number));
  } else {
    sql += ' ORDER BY COALESCE(w.theme_id, 2147483647) ASC, COALESCE(w.position, 2147483647) ASC, w.id ASC LIMIT 1';
  }

  return get(sql, params);
}

async function getWordPoolForUser(userId, filters = {}) {
  const mode = (filters.rev_mode || 'order').toLowerCase();
  const params = [userId, userId, userId, userId];
  let sql = `SELECT w.id
        FROM words w
        LEFT JOIN progress p ON p.word_id = w.id AND p.user_id = ?
        LEFT JOIN favorites fav ON fav.word_id = w.id AND fav.user_id = ?
        LEFT JOIN user_word_overrides uwo ON uwo.word_id = w.id AND uwo.user_id = ?
        LEFT JOIN themes t ON t.id = w.theme_id
        LEFT JOIN user_theme_overrides uto ON uto.theme_id = w.theme_id AND uto.user_id = ?
        LEFT JOIN theme_levels l ON l.id = w.level_id
        WHERE w.active = 1
          AND COALESCE(uwo.active, 1) = 1
          AND (w.theme_id IS NULL OR (t.active = 1 AND COALESCE(uto.active, 1) = 1))
          AND (w.level_id IS NULL OR l.active = 1)`;

  if (filters.theme_id) {
    sql += ' AND w.theme_id = ?';
    params.push(filters.theme_id);
  } else if (filters.theme_ids && filters.theme_ids.length > 0) {
    const placeholders = filters.theme_ids.map(() => '?').join(',');
    sql += ` AND w.theme_id IN (${placeholders})`;
    params.push(...filters.theme_ids);
  }
  if (filters.level_id) {
    sql += ' AND w.level_id = ?';
    params.push(filters.level_id);
  }
  if (filters.difficulty) {
    sql += ' AND w.difficulty = ?';
    params.push(filters.difficulty);
  }

  if (filters.scope === 'global') {
    sql += ' AND w.user_id IS NULL';
  } else if (filters.scope === 'mine') {
    sql += ' AND w.user_id = ?';
    params.push(userId);
  } else if (filters.scope === 'none') {
    return [];
  } else {
    sql += ' AND (w.user_id IS NULL OR w.user_id = ?)';
    params.push(userId);
  }

  if (filters.global_theme_ids && filters.global_theme_ids.length > 0 && filters.scope !== 'mine') {
    const placeholders = filters.global_theme_ids.map(() => '?').join(',');
    sql += ` AND (w.user_id = ? OR w.theme_id IN (${placeholders}))`;
    params.push(null, ...filters.global_theme_ids);
  }

  if (mode === 'favorites' || mode === 'favorite') {
    sql += ' AND fav.id IS NOT NULL';
  } else if (mode === 'new') {
    sql += ' AND p.id IS NULL';
  }

  sql += ' ORDER BY w.position ASC, w.id ASC';

  return all(sql, params);
}

async function fetchNextWord(userId, filters = {}) {
  const mode = (filters.rev_mode || 'order').toLowerCase();
  const params = [userId, userId, userId, userId];
  const excludedIds = Array.isArray(filters.excludedIds)
    ? filters.excludedIds.map(id => Number(id)).filter(id => !Number.isNaN(id))
    : [];
  let sql = `SELECT w.*,
        IFNULL(p.strength, 0) AS strength,
        p.last_seen,
        p.id AS progress_id,
        fav.id AS fav_id
       FROM words w
       LEFT JOIN progress p ON p.word_id = w.id AND p.user_id = ?
       LEFT JOIN favorites fav ON fav.word_id = w.id AND fav.user_id = ?
       LEFT JOIN user_word_overrides uwo ON uwo.word_id = w.id AND uwo.user_id = ?
       LEFT JOIN themes t ON t.id = w.theme_id
       LEFT JOIN user_theme_overrides uto ON uto.theme_id = w.theme_id AND uto.user_id = ?
       LEFT JOIN theme_levels l ON l.id = w.level_id
       WHERE w.active = 1
         AND COALESCE(uwo.active, 1) = 1
         AND (w.theme_id IS NULL OR (t.active = 1 AND COALESCE(uto.active, 1) = 1))
         AND (w.level_id IS NULL OR l.active = 1)`;

  if (filters.theme_id) {
    sql += ' AND w.theme_id = ?';
    params.push(filters.theme_id);
  } else if (filters.theme_ids && filters.theme_ids.length > 0) {
    const placeholders = filters.theme_ids.map(() => '?').join(',');
    sql += ` AND w.theme_id IN (${placeholders})`;
    params.push(...filters.theme_ids);
  }
  if (filters.level_id) {
    sql += ' AND w.level_id = ?';
    params.push(filters.level_id);
  }
  if (filters.difficulty) {
    sql += ' AND w.difficulty = ?';
    params.push(filters.difficulty);
  }

  if (filters.scope === 'global') {
    sql += ' AND w.user_id IS NULL';
  } else if (filters.scope === 'mine') {
    sql += ' AND w.user_id = ?';
    params.push(userId);
  } else if (filters.scope === 'none') {
    return null;
  } else {
    sql += ' AND (w.user_id IS NULL OR w.user_id = ?)';
    params.push(userId);
  }

  if (filters.global_theme_ids && filters.global_theme_ids.length > 0 && filters.scope !== 'mine') {
    const placeholders = filters.global_theme_ids.map(() => '?').join(',');
    sql += ` AND (w.user_id = ? OR w.theme_id IN (${placeholders}))`;
    params.push(null, ...filters.global_theme_ids);
  }

  if (mode === 'favorites' || mode === 'favorite') {
    sql += ' AND fav.id IS NOT NULL';
  } else if (mode === 'new') {
    sql += ' AND p.id IS NULL';
  }

  if (excludedIds.length > 0) {
    const placeholders = excludedIds.map(() => '?').join(',');
    sql += ` AND w.id NOT IN (${placeholders})`;
    params.push(...excludedIds);
  }

  if (mode === 'random' || mode === 'favorites' || mode === 'favorite') {
    sql += ' ORDER BY RANDOM() LIMIT 1';
  } else if (mode === 'new') {
    sql += ' ORDER BY w.created_at DESC LIMIT 1';
  } else if (mode === 'order') {
    sql += ' ORDER BY w.position ASC, w.id ASC LIMIT 1';
  } else {
    sql += ' ORDER BY strength ASC, IFNULL(p.last_seen, 0) ASC LIMIT 1';
  }

  return get(sql, params);
}

async function upsertProgress(userId, wordId, isCorrect) {
  const word = await get('SELECT * FROM words WHERE id = ?', [wordId]);
  if (!word) return null;

  const existing = await get(
    'SELECT * FROM progress WHERE user_id = ? AND word_id = ?',
    [userId, wordId]
  );

  let strength = existing ? existing.strength : 0;
  let success_count = existing ? existing.success_count : 0;
  let fail_count = existing ? existing.fail_count : 0;

  if (isCorrect) {
    strength = Math.min(100, strength + 10);
    success_count++;
  } else {
    strength = Math.max(0, strength - 10);
    fail_count++;
  }

  if (existing) {
    await run(
      `UPDATE progress
       SET strength = ?, success_count = ?, fail_count = ?, last_seen = CURRENT_TIMESTAMP
       WHERE id = ?`,
      [strength, success_count, fail_count, existing.id]
    );
  } else {
    await run(
      `INSERT INTO progress (user_id, word_id, strength, success_count, fail_count, last_seen)
       VALUES (?,?,?,?,?, CURRENT_TIMESTAMP)`,
      [userId, wordId, strength, success_count, fail_count]
    );
  }
  return { word, strength };
}

async function upsertCardProgress(userId, cardId, isCorrect) {
  const card = await get('SELECT * FROM cards WHERE id = ?', [cardId]);
  if (!card) return null;
  const existing = await get(
    'SELECT * FROM card_progress WHERE user_id = ? AND card_id = ?',
    [userId, cardId]
  );
  let success = existing ? existing.success_count : 0;
  let fail = existing ? existing.fail_count : 0;
  if (isCorrect) success++;
  else fail++;
  if (existing) {
    await run(
      `UPDATE card_progress
       SET success_count = ?, fail_count = ?, last_seen = CURRENT_TIMESTAMP
       WHERE id = ?`,
      [success, fail, existing.id]
    );
  } else {
    await run(
      `INSERT INTO card_progress (user_id, card_id, success_count, fail_count, last_seen)
       VALUES (?,?,?,?, CURRENT_TIMESTAMP)`,
      [userId, cardId, success, fail]
    );
  }
  return { card, success, fail };
}

async function getFlashcardOptions(userId, currentWord, filters = {}, reverse = false) {
  const params = [userId, userId, userId, currentWord.id];
  let sql = `SELECT w.id, w.french, w.hebrew
    FROM words w
    LEFT JOIN progress p ON p.word_id = w.id AND p.user_id = ?
    LEFT JOIN user_word_overrides uwo ON uwo.word_id = w.id AND uwo.user_id = ?
    LEFT JOIN themes t ON t.id = w.theme_id
    LEFT JOIN user_theme_overrides uto ON uto.theme_id = w.theme_id AND uto.user_id = ?
    LEFT JOIN theme_levels l ON l.id = w.level_id
    WHERE w.active = 1 AND w.id != ?
      AND COALESCE(uwo.active, 1) = 1
      AND (w.theme_id IS NULL OR (t.active = 1 AND COALESCE(uto.active, 1) = 1))
      AND (w.level_id IS NULL OR l.active = 1)`;

  if (filters.theme_id) {
    sql += ' AND w.theme_id = ?';
    params.push(filters.theme_id);
  } else if (filters.theme_ids && filters.theme_ids.length > 0) {
    const placeholders = filters.theme_ids.map(() => '?').join(',');
    sql += ` AND w.theme_id IN (${placeholders})`;
    params.push(...filters.theme_ids);
  }
  if (filters.level_id) {
    sql += ' AND w.level_id = ?';
    params.push(filters.level_id);
  }
  if (filters.difficulty) {
    sql += ' AND w.difficulty = ?';
    params.push(filters.difficulty);
  }

  if (filters.scope === 'global') {
    sql += ' AND w.user_id IS NULL';
  } else if (filters.scope === 'mine') {
    sql += ' AND w.user_id = ?';
    params.push(userId);
  } else {
    sql += ' AND (w.user_id IS NULL OR w.user_id = ?)';
    params.push(userId);
  }

  sql += ' ORDER BY RANDOM() LIMIT 3';
  const rows = await all(sql, params);
  const options = [
    ...rows.map(r => ({ id: r.id, french: r.french, hebrew: r.hebrew, label: reverse ? r.hebrew : r.french })),
    { id: currentWord.id, french: currentWord.french, hebrew: currentWord.hebrew, label: reverse ? currentWord.hebrew : currentWord.french }
  ];
  // shuffle
  for (let i = options.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [options[i], options[j]] = [options[j], options[i]];
  }
  return options;
}

async function getLevelsForUser(userId) {
  return all(
    `SELECT tl.*, t.name AS theme_name, t.user_id AS theme_user_id
     FROM theme_levels tl
     JOIN themes t ON t.id = tl.theme_id
     LEFT JOIN user_theme_overrides uto ON uto.theme_id = t.id AND uto.user_id = ?
     WHERE (t.user_id IS NULL OR t.user_id = ?) AND t.active = 1 AND COALESCE(uto.active, 1) = 1 AND tl.active = 1
     ORDER BY t.id ASC, tl.level_order, tl.id`,
    [userId, userId]
  );
}

async function getOwnedLevels(userId) {
  return all(
    `SELECT tl.*, t.name AS theme_name, t.user_id AS theme_user_id
     FROM theme_levels tl
     JOIN themes t ON t.id = tl.theme_id
     LEFT JOIN user_theme_overrides uto ON uto.theme_id = t.id AND uto.user_id = ?
     WHERE t.user_id = ? AND t.active = 1 AND COALESCE(uto.active, 1) = 1 AND tl.active = 1
     ORDER BY t.id ASC, tl.level_order, tl.id`,
    [userId, userId]
  );
}

async function normalizeLevelSelection(themeId, levelId) {
  const initialThemeId = themeId ? Number(themeId) : null;
  if (!levelId) return { levelId: null, themeId: initialThemeId };

  const level = await get('SELECT * FROM theme_levels WHERE id = ?', [levelId]);
  if (!level) return { levelId: null, themeId: initialThemeId };
  if (initialThemeId && Number(level.theme_id) !== initialThemeId) {
    return { levelId: null, themeId: initialThemeId };
  }
  return { levelId: level.id, themeId: initialThemeId || level.theme_id };
}

async function getThemeIdsInProgress(userId) {
  const rows = await all(
    `SELECT DISTINCT COALESCE(w.theme_id, tl.theme_id) AS theme_id
     FROM progress p
     JOIN words w ON w.id = p.word_id
     LEFT JOIN theme_levels tl ON tl.id = w.level_id
     WHERE p.user_id = ? AND COALESCE(w.theme_id, tl.theme_id) IS NOT NULL`,
    [userId]
  );
  return rows.map(r => r.theme_id);
}

function normalizeCardsPayload(rawCards) {
  const list = Array.isArray(rawCards) ? rawCards : rawCards ? [rawCards] : [];
  return list
    .map(card => ({
      id: card.id ? Number(card.id) : null,
      hebrew: card.hebrew ? card.hebrew.trim() : '',
      french: card.french ? card.french.trim() : '',
      transliteration: card.transliteration ? card.transliteration.trim() : ''
    }))
    .filter(card => card.hebrew && card.french);
}

// Normalise un tableau de mots/cartes importes depuis un JSON
function normalizeImportEntries(rawWords) {
  const list = Array.isArray(rawWords) ? rawWords : [];
  return list
    .map(w => ({
      hebrew: w.hebrew ? String(w.hebrew).trim() : '',
      french: w.french ? String(w.french).trim() : '',
      transliteration: w.transliteration ? String(w.transliteration).trim() : '',
      active: w.active === 0 ? 0 : 1,
      favorite: w.favorite ? 1 : 0,
      memorized: w.memorized ? 1 : 0
    }))
    .filter(w => w.hebrew && w.french);
}

function normalizeIds(raw) {
  const list = Array.isArray(raw) ? raw : raw ? [raw] : [];
  return list
    .map(id => Number(id))
    .filter(id => Number.isInteger(id) && id > 0);
}

function normalizeTrainParams(raw = {}) {
  const params = { ...DEFAULT_TRAIN_PARAMS };
  params.modes = normalizeModes(raw.modes || raw.mode);
  params.rev_mode = raw.rev_mode || params.rev_mode;
  params.show_phonetic =
    typeof raw.show_phonetic === 'undefined' ? params.show_phonetic : String(raw.show_phonetic) === '0' ? 0 : 1;
  params.theme_ids = normalizeIds(raw.theme_ids || []);
  params.set_ids = normalizeIds(raw.set_ids || []);
  params.level_id = raw.level_id ? Number(raw.level_id) : '';
  params.scope = raw.scope || params.scope;

  let remainingVal = raw.remaining === 'all' ? 'all' : Number(raw.remaining || params.remaining);
  if (remainingVal === 10) remainingVal = 'all'; // Legacy default migration

  params.remaining =
    remainingVal === 'all'
      ? 'all'
      : !Number.isNaN(remainingVal) && remainingVal > 0
        ? remainingVal
        : DEFAULT_TRAIN_PARAMS.remaining;
  const totalVal =
    raw.total === 'all' || params.remaining === 'all'
      ? 'all'
      : Number(raw.total || params.remaining || DEFAULT_TRAIN_PARAMS.total);
  params.total =
    totalVal === 'all'
      ? 'all'
      : !Number.isNaN(totalVal) && totalVal > 0
        ? totalVal
        : params.remaining;
  return params;
}

async function getUserTrainDefaults(userId) {
  const row = await get('SELECT prefs_json FROM user_revision_defaults WHERE user_id = ?', [userId]);
  if (!row) return null;
  try {
    return normalizeTrainParams(row.prefs_json ? JSON.parse(row.prefs_json) : {});
  } catch (e) {
    console.error('parse prefs error', e);
    return normalizeTrainParams({});
  }
}

async function saveUserTrainDefaults(userId, params = {}) {
  const normalized = normalizeTrainParams(params);
  const payload = JSON.stringify(normalized);
  const existing = await get('SELECT user_id FROM user_revision_defaults WHERE user_id = ?', [userId]);
  if (existing) {
    await run('UPDATE user_revision_defaults SET prefs_json = ? WHERE user_id = ?', [payload, userId]);
  } else {
    await run('INSERT INTO user_revision_defaults (user_id, prefs_json) VALUES (?,?)', [userId, payload]);
  }
  return normalized;
}

async function clearUserTrainDefaults(userId) {
  await run('DELETE FROM user_revision_defaults WHERE user_id = ?', [userId]);
}

async function filterTrainParamsForUser(userId, params = {}) {
  const normalized = normalizeTrainParams(params);
  const allowedThemeIds = new Set(await getActiveThemeIdsForUser(userId));
  const allowedSetIds = new Set(await getAccessibleSetIds(userId));
  normalized.theme_ids = normalized.theme_ids.filter(id => allowedThemeIds.has(Number(id)));
  normalized.set_ids = normalized.set_ids.filter(id => allowedSetIds.has(Number(id)));
  if (normalized.remaining === 'all') {
    normalized.total = 'all';
  } else if (normalized.total === 'all') {
    normalized.remaining = normalized.remaining || DEFAULT_TRAIN_PARAMS.remaining;
  } else if (!normalized.total || Number(normalized.total) <= 0) {
    normalized.total = normalized.remaining || DEFAULT_TRAIN_PARAMS.total;
  }
  return normalized;
}

function slugify(str = '') {
  return String(str)
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-zA-Z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .toLowerCase() || 'item';
}

function duplicateKey(hebrew, french) {
  const clean = (val = '') =>
    String(val)
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .toLowerCase()
      .trim();
  const fr = clean(french);
  const he = clean(hebrew);
  if (!fr && !he) return '';
  return `${fr}|${he}`;
}

function crc32(buf) {
  const table = crc32.table || (crc32.table = (() => {
    let c;
    const t = [];
    for (let n = 0; n < 256; n++) {
      c = n;
      for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
      t[n] = c >>> 0;
    }
    return t;
  })());
  let crc = 0 ^ -1;
  for (let i = 0; i < buf.length; i++) {
    crc = (crc >>> 0);
    crc = (crc >>> 8) ^ table[(crc ^ buf[i]) & 0xff];
  }
  return (crc ^ -1) >>> 0;
}

function createZip(files = []) {
  const encoder = new TextEncoder();
  const localParts = [];
  const centralParts = [];
  let offset = 0;
  const now = new Date();
  const dosTime = ((now.getHours() << 11) | (now.getMinutes() << 5) | (now.getSeconds() / 2)) & 0xffff;
  const dosDate = (((now.getFullYear() - 1980) << 9) | ((now.getMonth() + 1) << 5) | now.getDate()) & 0xffff;

  files.forEach(file => {
    const nameBuf = Buffer.from(encoder.encode(file.path));
    const dataBuf = Buffer.isBuffer(file.content)
      ? file.content
      : Buffer.from(typeof file.content === 'string' ? encoder.encode(file.content) : file.content);
    const csum = crc32(dataBuf);
    const local = Buffer.alloc(30 + nameBuf.length);
    let p = 0;
    local.writeUInt32LE(0x04034b50, p); p += 4; // local header signature
    local.writeUInt16LE(20, p); p += 2; // version needed
    local.writeUInt16LE(0, p); p += 2; // flags
    local.writeUInt16LE(0, p); p += 2; // compression (0 = store)
    local.writeUInt16LE(dosTime, p); p += 2;
    local.writeUInt16LE(dosDate, p); p += 2;
    local.writeUInt32LE(csum, p); p += 4;
    local.writeUInt32LE(dataBuf.length, p); p += 4;
    local.writeUInt32LE(dataBuf.length, p); p += 4;
    local.writeUInt16LE(nameBuf.length, p); p += 2;
    local.writeUInt16LE(0, p); p += 2; // extra length
    nameBuf.copy(local, p);
    const localOffset = offset;
    offset += local.length + dataBuf.length;
    localParts.push(local, dataBuf);

    const central = Buffer.alloc(46 + nameBuf.length);
    p = 0;
    central.writeUInt32LE(0x02014b50, p); p += 4; // central header signature
    central.writeUInt16LE(20, p); p += 2; // version made by
    central.writeUInt16LE(20, p); p += 2; // version needed
    central.writeUInt16LE(0, p); p += 2; // flags
    central.writeUInt16LE(0, p); p += 2; // compression
    central.writeUInt16LE(dosTime, p); p += 2;
    central.writeUInt16LE(dosDate, p); p += 2;
    central.writeUInt32LE(csum, p); p += 4;
    central.writeUInt32LE(dataBuf.length, p); p += 4;
    central.writeUInt32LE(dataBuf.length, p); p += 4;
    central.writeUInt16LE(nameBuf.length, p); p += 2;
    central.writeUInt16LE(0, p); p += 2; // extra len
    central.writeUInt16LE(0, p); p += 2; // comment len
    central.writeUInt16LE(0, p); p += 2; // disk
    central.writeUInt16LE(0, p); p += 2; // internal attr
    central.writeUInt32LE(0, p); p += 4; // external attr
    central.writeUInt32LE(localOffset, p); p += 4;
    nameBuf.copy(central, p);
    centralParts.push(central);
  });

  const centralSize = centralParts.reduce((sum, b) => sum + b.length, 0);
  const centralOffset = offset;
  const eocd = Buffer.alloc(22);
  let p = 0;
  eocd.writeUInt32LE(0x06054b50, p); p += 4;
  eocd.writeUInt16LE(0, p); p += 2; // disk
  eocd.writeUInt16LE(0, p); p += 2; // start disk
  eocd.writeUInt16LE(files.length, p); p += 2;
  eocd.writeUInt16LE(files.length, p); p += 2;
  eocd.writeUInt32LE(centralSize, p); p += 4;
  eocd.writeUInt32LE(centralOffset, p); p += 4;
  eocd.writeUInt16LE(0, p); p += 2; // comment len

  return Buffer.concat([...localParts, ...centralParts, eocd]);
}
async function getAccessibleSetIds(userId) {
  const rows = await all(
    `SELECT DISTINCT s.id
     FROM sets s
     LEFT JOIN set_shares sh ON sh.set_id = s.id AND sh.user_id = ?
     LEFT JOIN user_set_overrides uso ON uso.set_id = s.id AND uso.user_id = ?
     WHERE s.active = 1
       AND COALESCE(uso.active, s.active) = 1
       AND (s.user_id = ? OR sh.id IS NOT NULL)`,
    [userId, userId, userId]
  );
  return rows.map(r => Number(r.id));
}

async function getEffectiveCardsForUser(userId, setIds = []) {
  if (!setIds || setIds.length === 0) return [];
  const placeholders = setIds.map(() => '?').join(',');
  const rows = await all(
    `SELECT c.*,
      COALESCE(co.active, c.active) AS effective_active,
      COALESCE(co.favorite, c.favorite) AS effective_favorite,
      COALESCE(co.memorized, c.memorized) AS effective_memorized
     FROM cards c
     JOIN sets s ON s.id = c.set_id
     LEFT JOIN user_set_overrides uso ON uso.set_id = s.id AND uso.user_id = ?
     LEFT JOIN card_overrides co ON co.card_id = c.id AND co.user_id = ?
     WHERE c.set_id IN (${placeholders})
       AND s.active = 1
       AND COALESCE(uso.active, s.active) = 1`,
    [userId, userId, ...setIds]
  );
  return rows.filter(r => Number(r.effective_active) === 1).map(r => ({
    ...r,
    active: r.effective_active,
    favorite: r.effective_favorite,
    memorized: r.effective_memorized
  }));
}

async function getActiveThemeIdsForUser(userId) {
  const rows = await all(
    `SELECT DISTINCT t.id
     FROM themes t
     LEFT JOIN user_theme_overrides uto ON uto.theme_id = t.id AND uto.user_id = ?
     WHERE (t.user_id IS NULL OR t.user_id = ?)
       AND t.active = 1
       AND COALESCE(uto.active, 1) = 1`,
    [userId, userId]
  );
  return rows.map(r => Number(r.id));
}

async function getActiveThemesForUser(userId) {
  return all(
    `SELECT t.*, COALESCE(uto.active, t.active) AS effective_active, uto.active AS user_active
     FROM themes t
     LEFT JOIN user_theme_overrides uto ON uto.theme_id = t.id AND uto.user_id = ?
     WHERE (t.user_id IS NULL OR t.user_id = ?) AND t.active = 1 AND COALESCE(uto.active, 1) = 1
     ORDER BY t.id ASC`,
    [userId, userId]
  );
}

async function getActiveThemeTreeForUser(userId) {
  const raw = await getActiveThemesForUser(userId);
  const activeIds = new Set(raw.map(t => Number(t.id)));
  const filtered = raw.filter(t => !t.parent_id || activeIds.has(Number(t.parent_id)));

  const sortByIdAsc = (a, b) => Number(a.id) - Number(b.id);

  const themeMap = new Map();
  const roots = [];

  filtered.forEach(t => {
    t.children = [];
    themeMap.set(t.id, t);
  });

  filtered.forEach(t => {
    if (t.parent_id && themeMap.has(t.parent_id)) {
      themeMap.get(t.parent_id).children.push(t);
    } else {
      roots.push(t);
    }
  });

  roots.sort(sortByIdAsc);
  filtered.forEach(t => t.children.sort(sortByIdAsc));

  return roots;
}

const app = express();
const PORT = process.env.PORT || 3000;

// EJS + layouts
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));
app.use(expressLayouts);
app.set('layout', 'layout');

// Middlewares
app.use(express.static(path.join(__dirname, 'public')));
app.use(
  express.urlencoded({
    extended: true,
    parameterLimit: 2000,
    limit: '1mb'
  })
);
app.use(methodOverride('_method'));

app.use(
  session({
    store: new SQLiteStore({ db: 'sessions.db', dir: '.' }),
    secret: 'change-moi-en-secret-solide',
    resave: false,
    saveUninitialized: false,
    cookie: { maxAge: 1000 * 60 * 60 }
  })
);

// Utilisateur accessible dans les vues
app.use((req, res, next) => {
  res.locals.currentUser = req.session.user || null;
  next();
});

async function reloadUser(req, res) {
  if (!req.session.user) return null;
  const fresh = await get('SELECT * FROM users WHERE id = ?', [req.session.user.id]);
  if (!fresh) return null;
  const normalized = {
    id: fresh.id,
    email: fresh.email,
    display_name: fresh.display_name,
    first_name: fresh.first_name,
    last_name: fresh.last_name,
    level: fresh.level || 'Débutant',
    role: fresh.role,
    theme: fresh.theme || 'dark'
  };
  req.session.user = normalized;
  res.locals.currentUser = normalized;
  return normalized;
}

async function nextDisplayNo(table, userIdCondition) {
  let sql = `SELECT MAX(display_no) AS max_no FROM ${table} WHERE `;
  const params = [];
  if (userIdCondition === null) {
    sql += 'user_id IS NULL';
  } else {
    sql += 'user_id = ?';
    params.push(userIdCondition);
  }
  const row = await get(sql, params);
  return (row && row.max_no ? Number(row.max_no) : 0) + 1;
}

async function nextWordPosition(themeId, userId) {
  if (!themeId) return 1;
  const params = [themeId];
  let sql = 'SELECT COALESCE(MAX(position), 0) AS pos FROM words WHERE theme_id = ?';
  if (userId === null) {
    sql += ' AND user_id IS NULL';
  } else {
    sql += ' AND user_id = ?';
    params.push(userId);
  }
  const row = await get(sql, params);
  return (row && row.pos ? Number(row.pos) : 0) + 1;
}

async function renumberThemes(userId) {
  const rows = await all(
    userId === null
      ? 'SELECT id FROM themes WHERE user_id IS NULL ORDER BY COALESCE(created_at, id) ASC'
      : 'SELECT id FROM themes WHERE user_id = ? ORDER BY COALESCE(created_at, id) ASC',
    userId === null ? [] : [userId]
  );
  for (let i = 0; i < rows.length; i++) {
    await run('UPDATE themes SET display_no = ? WHERE id = ?', [i + 1, rows[i].id]);
  }
}

async function renumberSets(userId) {
  const rows = await all(
    'SELECT id FROM sets WHERE user_id = ? ORDER BY COALESCE(created_at, id) ASC',
    [userId]
  );
  for (let i = 0; i < rows.length; i++) {
    await run('UPDATE sets SET display_no = ? WHERE id = ?', [i + 1, rows[i].id]);
  }
}

async function renumberAllThemesAndSets() {
  const themeOwners = await all('SELECT DISTINCT user_id FROM themes');
  const owners = themeOwners.map(r => r.user_id);
  if (!owners.includes(null)) owners.push(null);
  for (const owner of owners) {
    await renumberThemes(owner);
  }
  const setOwners = await all('SELECT DISTINCT user_id FROM sets');
  for (const row of setOwners) {
    await renumberSets(row.user_id);
  }
}

async function requireAuth(req, res, next) {
  if (!req.session.user) return res.redirect('/login');
  try {
    const updated = await reloadUser(req, res);
    if (!updated) {
      req.session.destroy(() => { });
      return res.redirect('/login');
    }
    next();
  } catch (err) {
    console.error(err);
    res.redirect('/login');
  }
}

async function requireAdmin(req, res, next) {
  if (!req.session.user) return res.redirect('/login');
  try {
    const updated = await reloadUser(req, res);
    if (!updated || updated.role !== 'admin') {
      return res.status(403).send('Accès refusé');
    }
    next();
  } catch (err) {
    console.error(err);
    res.status(403).send('Accès refusé');
  }
}

// Admin par dÃ©faut
async function ensureAdmin() {
  const admin = await get("SELECT * FROM users WHERE role = 'admin'");
  if (!admin) {
    const hash = await bcrypt.hash('admin123', 10);
    await run(
      'INSERT INTO users (email, password_hash, password_plain, display_name, first_name, last_name, level, role) VALUES (?,?,?,?,?,?,?,?)',
      ['admin@example.com', hash, 'admin123', 'Admin', 'Admin', 'System', 'Expert', 'admin']
    );
    console.log('Admin crÃ©Ã© : admin@example.com / admin123');
  }
}

// Themes de base
async function ensureBaseThemes() {
  const row = await get('SELECT COUNT(*) AS c FROM themes', []);
  const count = row ? row.c : 0;
  if (count === 0) {
    const seedThemes = [
      'Immigration et Alya',
      'Politique et droits',
      'Education',
      'Environnement, \u00e9cologie',
      'Monde du travail',
      'Magasins et achats',
      'La parole et les 5 sens',
      'La guerre et l\'arm\u00e9e',
      'Les sentiments',
      'Description d\'un objet',
      'Description du caract\u00e8re',
      'Description du physique',
      'Les voyages',
      'Description d\'un lieu',
      'Compter et mesurer',
      'Justice et crimes',
      'Rep\u00e8res temporels',
      'Banque, bourse, affaires',
      'Maison et nettoyage',
      'Sport et jeux',
      'Climat et m\u00e9t\u00e9o',
      'Poids et mesures',
      'Famille et cycle de la vie',
      'Agriculture',
      'La presse',
      'Informatique et Internet',
      'V\u00eatements',
      'T\u00e9l\u00e9phone portable',
      'Art et culture',
      'Le monde du spectacle',
      'Automobile',
      'La vie juive',
      'Sant\u00e9',
      'Loisirs',
      'Animaux',
      'Appareils m\u00e9nagers',
      'La ville et ses probl\u00e8mes',
      'Probl\u00e8mes de soci\u00e9t\u00e9',
      'Mots \u00e0 conna\u00eetre',
      'Les mots pour commenter',
      'Alimentation'
    ];
    let idx = 1;
    for (const name of seedThemes) {
      const created = await run('INSERT INTO themes (name, active, created_at, display_no) VALUES (?,1, CURRENT_TIMESTAMP, ?)', [name, idx]);
      await run('INSERT INTO theme_levels (theme_id, name, level_order, active) VALUES (?,?,?,1)', [created.lastID, 'Niveau 1', 1]);
      idx += 1;
    }
    console.log('Thèmes de base crées (liste fournie).');
  }
}
(async () => {
  try {
    await ensureAdmin();
    await renumberAllThemesAndSets();
  } catch (e) {
    console.error('Erreur init DB :', e);
  }
})();

// Routes
app.get('/', (req, res) => {
  if (req.session.user) return res.redirect('/app');
  res.redirect('/login');
});

// ---------- Auth ----------
app.get('/register', (req, res) => {
  res.render('register', { error: null });
});

app.post('/register', async (req, res) => {
  const email = (req.body.email || '').trim();
  const password = req.body.password || '';
  const firstName = (req.body.first_name || '').trim();
  const lastNameClean = (req.body.last_name || '').trim();
  const displayRaw = (req.body.display_name || '').trim();
  const level = (req.body.level || '').trim();
  const displayNameValue =
    displayRaw || [firstName, lastNameClean].filter(Boolean).join(' ').trim();
  if (!email || !password || !firstName || !displayNameValue || !level) {
    return res.render('register', { error: 'Tous les champs sont obligatoires.' });
  }
  try {
    const existing = await get('SELECT * FROM users WHERE email = ?', [email]);
    if (existing) {
      return res.render('register', { error: 'Cet email est déjà  utilisé.' });
    }
    const hash = await bcrypt.hash(password, 10);
    const info = await run(
      'INSERT INTO users (email, password_hash, password_plain, display_name, first_name, last_name, level, role) VALUES (?,?,?,?,?,?,?,?)',
      [email, hash, password, displayNameValue, firstName, lastNameClean, level, 'user']
    );
    req.session.user = {
      id: info.lastID,
      email,
      display_name: displayNameValue,
      first_name: firstName,
      last_name: lastNameClean,
      level,
      role: 'user'
    };
    res.redirect('/app');
  } catch (e) {
    console.error(e);
    res.render('register', { error: 'Erreur serveur.' });
  }
});

app.get('/login', (req, res) => {
  res.render('login', { error: null });
});

app.post('/login', async (req, res) => {
  const { email, password } = req.body;
  try {
    const user = await get('SELECT * FROM users WHERE email = ?', [email]);
    if (!user) {
      return res.render('login', { error: 'Identifiants invalides.' });
    }
    const ok = await bcrypt.compare(password, user.password_hash);
    if (!ok) {
      return res.render('login', { error: 'Identifiants invalides.' });
    }
    req.session.user = {
      id: user.id,
      email: user.email,
      display_name: user.display_name,
      first_name: user.first_name,
      last_name: user.last_name,
      level: user.level || 'Debutant',
      role: user.role
    };
    res.redirect('/app');
  } catch (e) {
    console.error(e);
    res.render('login', { error: 'Erreur serveur.' });
  }
});

app.post('/logout', (req, res) => {
  req.session.destroy(() => res.redirect('/login'));
});

// ---------- Dashboard utilisateur ---------
app.get('/app', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  try {
    const statsWords = await get(
      `SELECT
        COUNT(DISTINCT word_id) AS words_seen,
        SUM(success_count) AS total_success,
        SUM(fail_count) AS total_fail,
        AVG(strength) AS avg_strength,
        COUNT(*) AS progress_count
       FROM progress WHERE user_id = ?`,
      [userId]
    );
    const statsCards = await get(
      `SELECT
        COUNT(DISTINCT cp.card_id) AS cards_seen,
        SUM(cp.success_count) AS total_success,
        SUM(cp.fail_count) AS total_fail
       FROM card_progress cp
       JOIN cards c ON c.id = cp.card_id
       JOIN sets s ON s.id = c.set_id AND s.active = 1
       LEFT JOIN set_shares sh ON sh.set_id = s.id AND sh.user_id = ?
       WHERE cp.user_id = ? AND (s.user_id = ? OR sh.id IS NOT NULL)`,
      [userId, userId, userId]
    );
    const stats = {
      words_seen: statsWords?.words_seen || 0,
      cards_seen: statsCards?.cards_seen || 0,
      total_success: (statsWords?.total_success || 0) + (statsCards?.total_success || 0),
      total_fail: (statsWords?.total_fail || 0) + (statsCards?.total_fail || 0),
      avg_strength: 0
    };
    const attempts = stats.total_success + stats.total_fail;
    if (attempts > 0) {
      stats.avg_strength = Math.round((stats.total_success / attempts) * 100);
    }
    stats.total_seen = stats.words_seen + stats.cards_seen;

    let ongoing = null;
    const state = req.session.trainState;
    if (state && Number(state.remaining || 0) > 0) {
      const themeIds = state.theme_ids || [];
      const setIds = state.set_ids || [];
      let themeNames = [];
      let setNames = [];
      if (themeIds.length > 0) {
        const ph = themeIds.map(() => '?').join(',');
        themeNames = await all(`SELECT id, name FROM themes WHERE id IN (${ph})`, themeIds);
      }
      if (setIds.length > 0) {
        const ph = setIds.map(() => '?').join(',');
        setNames = await all(`SELECT id, name FROM sets WHERE id IN (${ph})`, setIds);
      }
      ongoing = {
        modes: state.modes && state.modes.length ? state.modes : [state.mode || 'flashcards'],
        remaining: state.remaining,
        total: state.total,
        answered: state.answered || 0,
        correct: state.correct || 0,
        themes: themeNames,
        sets: setNames
      };
    }

    res.render('app', { stats, themes: [], ongoing });
  } catch (e) {
    console.error(e);
    res.render('app', { stats: null, themes: [], ongoing: null });
  }
});

// ---------- Profil ----------
app.get('/profile', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  try {
    const user = await get('SELECT * FROM users WHERE id = ?', [userId]);
    const stats = await get(
      `SELECT
        COUNT(DISTINCT word_id) AS words_seen,
        SUM(success_count) AS total_success,
        SUM(fail_count) AS total_fail,
        AVG(strength) AS avg_strength
       FROM progress WHERE user_id = ?`,
      [userId]
    );
    const trainThemes = await getActiveThemesForUser(userId);
    const trainLevels = await getLevelsForUser(userId);
    const trainSets = await all(
      `SELECT s.id, s.name
       FROM sets s
       LEFT JOIN user_set_overrides uso ON uso.set_id = s.id AND uso.user_id = ?
       WHERE s.user_id = ? AND s.active = 1 AND COALESCE(uso.active, s.active) = 1
       ORDER BY s.created_at DESC`,
      [userId, userId]
    );
    const trainSharedSets = await all(
      `SELECT s.id, s.name, owner.display_name AS owner_name
       FROM set_shares sh
       JOIN sets s ON s.id = sh.set_id
       LEFT JOIN user_set_overrides uso ON uso.set_id = s.id AND uso.user_id = ?
       JOIN users owner ON owner.id = s.user_id
       WHERE sh.user_id = ? AND s.active = 1 AND COALESCE(uso.active, s.active) = 1
       ORDER BY s.created_at DESC`,
      [userId, userId]
    );
    const storedDefaults = await getUserTrainDefaults(userId);
    const defaultParams = await filterTrainParamsForUser(userId, storedDefaults || DEFAULT_TRAIN_PARAMS);
    const levelValue = user && user.level ? user.level : 'Debutant';
    res.render('profile', {
      user: { ...user, level: levelValue },
      stats,
      levels: ['Debutant', 'Intermediaire', 'Avance', 'Expert'],
      message: req.query.msg || null,
      error: req.query.err || null,
      pwdMessage: req.query.pwd || null,
      defaultParams,
      trainThemes,
      trainLevels,
      trainSets,
      trainSharedSets,
      prefMessage: req.query.pref || null,
      prefError: req.query.preferr || null
    });
  } catch (e) {
    console.error(e);
    res.render('profile', {
      user: req.session.user,
      stats: null,
      levels: ['Debutant', 'Intermediaire', 'Avance', 'Expert'],
      message: null,
      error: 'Impossible de charger le profil.',
      pwdMessage: null,
      defaultParams: DEFAULT_TRAIN_PARAMS,
      trainThemes: [],
      trainLevels: [],
      trainSets: [],
      trainSharedSets: [],
      prefMessage: null,
      prefError: 'Impossible de charger les preferences.'
    });
  }
});

app.post('/profile/info', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const firstName = (req.body.first_name || '').trim();
  const lastNameClean = (req.body.last_name || '').trim();
  const displayRaw = (req.body.display_name || '').trim();
  const email = (req.body.email || '').trim();
  const level = (req.body.level || '').trim();
  const displayNameValue = displayRaw || [firstName, lastNameClean].filter(Boolean).join(' ').trim();
  if (!firstName || !displayNameValue || !email || !level) {
    return res.redirect('/profile?err=Champs manquants');
  }
  try {
    const existing = await get('SELECT id FROM users WHERE email = ? AND id <> ?', [email, userId]);
    if (existing) {
      return res.redirect('/profile?err=Email déjà utilisé');
    }
    await run(
      'UPDATE users SET first_name = ?, last_name = ?, display_name = ?, email = ?, level = ? WHERE id = ?',
      [firstName, lastNameClean, displayNameValue, email, level, userId]
    );
    req.session.user = {
      ...req.session.user,
      first_name: firstName,
      last_name: lastNameClean,
      display_name: displayNameValue,
      email,
      level
    };
    res.redirect('/profile?msg=Profil mis a jour');
  } catch (e) {
    console.error(e);
    res.redirect('/profile?err=Erreur de mise a jour');
  }
});

app.post('/profile/theme', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const theme = req.body.theme === 'light' ? 'light' : 'dark';
  try {
    await run('UPDATE users SET theme = ? WHERE id = ?', [theme, userId]);
    req.session.user.theme = theme;
    res.redirect('/profile?msg=Theme mis a jour');
  } catch (e) {
    console.error(e);
    res.redirect('/profile?err=Erreur de mise a jour du theme');
  }
});

app.post('/profile/revision-defaults', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const { rev_mode, level_id, remaining, total, show_phonetic } = req.body;
  const rawModes = req.body.modes || req.body['modes[]'] || req.body.mode;
  const modeList = normalizeModes(rawModes);
  const themeRaw = Array.isArray(req.body.theme_ids) ? req.body.theme_ids.filter(Boolean) : req.body.theme_ids ? [req.body.theme_ids] : [];
  const setRaw = Array.isArray(req.body.set_ids) ? req.body.set_ids.filter(Boolean) : req.body.set_ids ? [req.body.set_ids] : [];
  const themeIds = themeRaw
    .flatMap(t => String(t).split(','))
    .map(id => Number(id))
    .filter(id => !Number.isNaN(id));
  const setIds = setRaw
    .flatMap(s => String(s).split(','))
    .map(id => Number(id))
    .filter(id => !Number.isNaN(id));

  const params = {
    modes: modeList,
    rev_mode: rev_mode || DEFAULT_TRAIN_PARAMS.rev_mode,
    theme_ids: themeIds,
    set_ids: setIds,
    level_id: level_id || '',
    remaining: remaining,
    total: total,
    show_phonetic
  };

  try {
    const filtered = await filterTrainParamsForUser(userId, params);
    await saveUserTrainDefaults(userId, filtered);
    return res.redirect('/profile?pref=' + encodeURIComponent('Paramètres enregistrés'));
  } catch (e) {
    console.error(e);
    return res.redirect('/profile?preferr=' + encodeURIComponent('Impossible de sauver les paramètres'));
  }
});

app.post('/profile/revision-defaults/reset', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  try {
    await clearUserTrainDefaults(userId);
    return res.redirect('/profile?pref=' + encodeURIComponent('Paramètres remis par défaut'));
  } catch (e) {
    console.error(e);
    return res.redirect('/profile?preferr=' + encodeURIComponent('Impossible de reinitialiser'));
  }
});

app.post('/profile/password', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const { current_password, new_password } = req.body;
  if (!current_password || !new_password) {
    return res.redirect('/profile?err=Champs manquants');
  }
  try {
    const user = await get('SELECT password_hash FROM users WHERE id = ?', [userId]);
    if (!user) return res.redirect('/profile?err=Profil introuvable');
    const ok = await bcrypt.compare(current_password, user.password_hash);
    if (!ok) return res.redirect('/profile?err=Mot de passe invalide');
    const hash = await bcrypt.hash(new_password, 10);
    await run('UPDATE users SET password_hash = ?, password_plain = ? WHERE id = ?', [
      hash,
      new_password,
      userId
    ]);
    res.redirect('/profile?pwd=Mot de passe mis a jour');
  } catch (e) {
    console.error(e);
    res.redirect('/profile?err=Erreur lors du changement de mot de passe');
  }
});

app.post('/profile/reset-stats', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  try {
    await run('DELETE FROM progress WHERE user_id = ?', [userId]);
    await run('DELETE FROM card_progress WHERE user_id = ?', [userId]);
    res.redirect('/profile?msg=Statistiques reinitialisees');
  } catch (e) {
    console.error(e);
    res.redirect('/profile?err=Impossible de reinitialiser');
  }
});

app.post('/profile/delete', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const { password } = req.body;
  if (!password) return res.redirect('/profile?err=Mot de passe requis');
  try {
    const user = await get('SELECT password_hash FROM users WHERE id = ?', [userId]);
    if (!user) return res.redirect('/profile?err=Profil introuvable');
    const ok = await bcrypt.compare(password, user.password_hash);
    if (!ok) return res.redirect('/profile?err=Mot de passe invalide');
    await run('DELETE FROM favorites WHERE user_id = ?', [userId]);
    await run('DELETE FROM user_word_overrides WHERE user_id = ?', [userId]);
    await run('DELETE FROM user_set_overrides WHERE user_id = ?', [userId]);
    await run('DELETE FROM user_theme_overrides WHERE user_id = ?', [userId]);
    await run('DELETE FROM card_overrides WHERE user_id = ?', [userId]);
    await run('DELETE FROM set_shares WHERE user_id = ?', [userId]);
    await run('DELETE FROM progress WHERE user_id = ?', [userId]);
    await run('DELETE FROM card_progress WHERE user_id = ?', [userId]);
    await run('DELETE FROM users WHERE id = ?', [userId]);
    req.session.destroy(() => {
      res.redirect('/register');
    });
  } catch (e) {
    console.error(e);
    res.redirect('/profile?err=Erreur lors de la suppression');
  }
});

// ---------- Import JSON ----------
function buildImportContext(req, extra = {}) {
  const isAdmin = req.session.user && req.session.user.role === 'admin';
  return {
    isAdmin,
    importType: isAdmin ? 'theme' : 'list',
    pageClass: 'page-compact',
    importError: null,
    importSuccess: null,
    payloadValue: '',
    pendingImport: null,
    ...extra
  };
}

app.get('/import', requireAuth, (req, res) => {
  const ctx = buildImportContext(req, {
    importError: req.query.import_error || null,
    importSuccess: req.query.import_success || null
  });
  res.render('import', ctx);
});

app.post('/import', requireAuth, async (req, res) => {
  const isAdmin = req.session.user && req.session.user.role === 'admin';
  const importType = isAdmin ? 'theme' : 'list';
  const redirectErr = msg => res.redirect(`/import?import_error=${encodeURIComponent(msg)}`);
  const payloadText = typeof req.body.payload === 'string' ? req.body.payload.trim() : '';
  if (!payloadText) return redirectErr('Fichier JSON manquant');

  let parsed;
  try {
    parsed = JSON.parse(payloadText);
  } catch (e) {
    return redirectErr('JSON invalide');
  }
  const name = parsed && parsed.name ? String(parsed.name).trim() : '';
  if (!name) return redirectErr(importType === 'theme' ? 'Nom de theme manquant' : 'Nom de liste manquant');

  const words = normalizeImportEntries(parsed.words);
  if (words.length === 0) return redirectErr('Aucun mot valide dans le fichier');
  const active = parsed.active === 0 ? 0 : 1;
  const confirmAppend = req.body.confirm === 'append';

  try {
    if (importType === 'theme') {
      const existing = await get('SELECT * FROM themes WHERE name = ? AND user_id IS NULL', [name]);
      if (existing && !confirmAppend) {
        return res.render(
          'import',
          buildImportContext(req, {
            payloadValue: payloadText,
            pendingImport: { type: 'theme', name, targetId: existing.id, wordsCount: words.length }
          })
        );
      }

      let parentId = null;
      if (parsed.parent_name) {
        const parentName = parsed.parent_name.trim();
        let parent = await get('SELECT id FROM themes WHERE name = ? AND user_id IS NULL', [parentName]);
        if (!parent) {
          // Auto-create parent theme
          const newParentId = await findFirstAvailableId('themes');
          const parentDisplayNo = await nextDisplayNo('themes', null);
          await run(
            'INSERT INTO themes (id, name, active, user_id, created_at, display_no, parent_id) VALUES (?,?,?,NULL, CURRENT_TIMESTAMP, ?, NULL)',
            [newParentId, parentName, 1, parentDisplayNo]
          );
          parentId = newParentId;
        } else {
          parentId = parent.id;
        }
      }

      const newId = await findFirstAvailableId('themes');
      const themeId = existing
        ? existing.id
        : (await run('INSERT INTO themes (id, name, active, user_id, created_at, display_no, parent_id) VALUES (?,?,?,NULL, CURRENT_TIMESTAMP, ?, ?)', [newId, name, active, await nextDisplayNo('themes', null), parentId])).lastID;

      const allocateWordId = await createIdAllocator('words');
      let positionSeed = existing ? await nextWordPosition(themeId, null) : 1;
      for (let i = 0; i < words.length; i++) {
        const w = words[i];
        const position = positionSeed + i;
        const wordId = allocateWordId();
        await run(
          `INSERT INTO words (id, hebrew, transliteration, french, theme_id, level_id, difficulty, active, user_id, position)
           VALUES (?,?,?,?,?,?,?,?,?,?)`,
          [wordId, w.hebrew, w.transliteration || null, w.french, themeId, null, 1, w.active, null, position]
        );
      }
      const msg = existing ? 'Mots ajoutes au theme existant' : 'Theme importe avec succes';
      return res.redirect(`/import?import_success=${encodeURIComponent(msg)}`);
    }

    const userId = req.session.user.id;
    const existing = await get('SELECT * FROM sets WHERE name = ? AND user_id = ?', [name, userId]);
    if (existing && !confirmAppend) {
      return res.render(
        'import',
        buildImportContext(req, {
          payloadValue: payloadText,
          pendingImport: { type: 'list', name, targetId: existing.id, wordsCount: words.length }
        })
      );
    }

    const newId = await findFirstAvailableId('sets');
    const setId = existing
      ? existing.id
      : (await run('INSERT INTO sets (id, name, user_id, active, display_no) VALUES (?,?,?,?,?)', [newId, name, userId, active ? 1 : 0, await nextDisplayNo('sets', userId)])).lastID;

    const posRow = await get('SELECT MAX(position) AS pos FROM cards WHERE set_id = ?', [setId]);
    const startPos = posRow && posRow.pos ? Number(posRow.pos) : 0;

    for (let i = 0; i < words.length; i++) {
      const w = words[i];
      await run(
        `INSERT INTO cards (set_id, french, hebrew, transliteration, position, active, favorite, memorized)
         VALUES (?,?,?,?,?,?,?,?)`,
        [setId, w.french, w.hebrew, w.transliteration || null, startPos + i + 1, w.active, w.favorite, w.memorized]
      );
    }
    const msg = existing ? 'Cartes ajoutees a la liste existante' : 'Liste importee avec succes';
    return res.redirect(`/import?import_success=${encodeURIComponent(msg)}`);
  } catch (e) {
    console.error('Import error:', e);
    return res.redirect(`/import?import_error=${encodeURIComponent('Erreur pendant l import')}`);
  }
});

app.post('/export', requireAuth, async (req, res) => {
  const isAdmin = req.session.user && req.session.user.role === 'admin';
  const userId = req.session.user.id;
  try {
    const files = [];

    const addListFile = (basePath, set, cards) => {
      const payload = {
        name: set.name,
        active: set.active,
        words: cards.map(c => ({
          hebrew: c.hebrew,
          french: c.french,
          transliteration: c.transliteration,
          active: c.active,
          favorite: c.favorite,
          memorized: c.memorized
        }))
      };
      const fname = `${basePath}/list_${set.id}_${slugify(set.name)}.json`;
      files.push({ path: fname, content: JSON.stringify(payload, null, 2) });
    };

    if (isAdmin) {
      const themes = await all('SELECT t.*, p.name AS parent_name FROM themes t LEFT JOIN themes p ON p.id = t.parent_id');
      for (const t of themes) {
        const words = await all('SELECT hebrew, french, transliteration, active, difficulty FROM words WHERE theme_id = ? ORDER BY position ASC, id ASC', [t.id]);
        const payload = {
          name: t.name,
          active: t.active,
          ...(t.parent_id ? { parent_name: t.parent_name } : {}),
          words: words.map(w => ({
            hebrew: w.hebrew,
            french: w.french,
            transliteration: w.transliteration,
            active: w.active,
            difficulty: w.difficulty
          }))
        };
        const fname = `themes/theme_${t.id}_${slugify(t.name)}.json`;
        files.push({ path: fname, content: JSON.stringify(payload, null, 2) });
      }

      const sets = await all('SELECT s.*, u.display_name AS owner_name FROM sets s JOIN users u ON u.id = s.user_id');
      for (const s of sets) {
        const cards = await all('SELECT hebrew, french, transliteration, active, favorite, memorized FROM cards WHERE set_id = ? ORDER BY position ASC, id ASC', [s.id]);
        addListFile(`lists/user_${s.user_id}_${slugify(s.owner_name || '')}`, s, cards);
      }
    } else {
      const sets = await all('SELECT * FROM sets WHERE user_id = ?', [userId]);
      for (const s of sets) {
        const cards = await all('SELECT hebrew, french, transliteration, active, favorite, memorized FROM cards WHERE set_id = ? ORDER BY position ASC, id ASC', [s.id]);
        addListFile('my-lists', s, cards);
      }
    }

    const zipBuf = createZip(files);
    const filename = isAdmin ? 'export-admin.zip' : 'export-mes-listes.zip';
    res.setHeader('Content-Type', 'application/zip');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    return res.end(zipBuf);
  } catch (e) {
    console.error('Export error:', e);
    return res.status(500).send('Erreur pendant l export');
  }
});


// ---------- BibliothÃ¨que perso / globale ----------
app.get('/my/words', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  try {
    const personalWords = await all(
      `SELECT w.*, t.name AS theme_name, l.name AS level_name, f.id AS fav_id
       FROM words w
       LEFT JOIN themes t ON t.id = w.theme_id
       LEFT JOIN theme_levels l ON l.id = w.level_id
       LEFT JOIN favorites f ON f.word_id = w.id AND f.user_id = ?
        WHERE w.user_id = ?
       ORDER BY w.id ASC`,
      [userId, userId]
    );
    const favorites = await all(
      `SELECT w.*, t.name AS theme_name, l.name AS level_name, f.id AS fav_id
       FROM favorites f
       JOIN words w ON w.id = f.word_id
       LEFT JOIN themes t ON t.id = w.theme_id
       LEFT JOIN theme_levels l ON l.id = w.level_id
       WHERE f.user_id = ?
       ORDER BY w.id ASC`,
      [userId]
    );
    const listFavorites = await all(
      `SELECT c.*, s.name AS set_name
       FROM cards c
       JOIN sets s ON s.id = c.set_id
       WHERE s.user_id = ? AND c.favorite = 1
       ORDER BY c.id ASC`,
      [userId]
    );
    const sharedFavorites = await all(
      `SELECT c.*, s.name AS set_name, owner.display_name AS owner_name
       FROM set_shares sh
       JOIN cards c ON c.set_id = sh.set_id
       LEFT JOIN card_overrides co ON co.card_id = c.id AND co.user_id = sh.user_id
       JOIN sets s ON s.id = c.set_id
       JOIN users owner ON owner.id = s.user_id
       WHERE sh.user_id = ?
         AND (
           (co.favorite = 1) OR (co.favorite IS NULL AND c.favorite = 1 AND sh.can_edit = 1)
         )
       ORDER BY c.id ASC`,
      [userId]
    );
    const themes = await all('SELECT * FROM themes WHERE user_id = ? AND active = 1 ORDER BY id ASC', [userId]);
    res.render('my_words', { globalWords: [], personalWords, favorites, themes, listFavorites, sharedFavorites });
  } catch (e) {
    console.error(e);
    res.render('my_words', { globalWords: [], personalWords: [], favorites: [], themes: [], listFavorites: [], sharedFavorites: [] });
  }
});

app.post('/train/flashcards', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const { word_id, choice_word_id, question_mode } = req.body;
  const state = req.session.trainState;
  if (!state) return res.redirect('/train/setup');

  const modeList = normalizeModes(state.modes || state.mode);
  state.modes = modeList;
  const activeMode = normalizeMode(question_mode || state.currentMode || modeList[0]);
  const allowedThemeIds = new Set(await getActiveThemeIdsForUser(userId));
  const filteredThemeIds = (state.theme_ids || []).filter(id => allowedThemeIds.has(Number(id)));
  state.theme_ids = filteredThemeIds;
  const selectedSetIds = state.set_ids || [];
  const source = state.source || (selectedSetIds.length > 0 ? 'cards' : 'words');
  const filters = {
    theme_id: filteredThemeIds.length === 1 ? filteredThemeIds[0] : null,
    theme_ids: filteredThemeIds,
    level_id: state.level_id || null,
    show_phonetic: typeof state.show_phonetic === 'undefined' ? 1 : state.show_phonetic,
    rev_mode: state.rev_mode || 'random',
    scope: state.scope || 'all',
    scope_list: [],
    source,
    set_ids: selectedSetIds,
    theme_order: filteredThemeIds
  };

  const prevTotal = Number(state.total);
  const totalCount = Number.isFinite(prevTotal) && prevTotal > 0 ? prevTotal : (Number.isFinite(Number(state.remaining)) ? Number(state.remaining) : 1);
  const prevRemaining = Number(state.remaining);
  const remainingBefore = Number.isFinite(prevRemaining) ? prevRemaining : totalCount;
  const remainingCount = Math.max(0, remainingBefore - 1);
  const prevAnswered = Number(state.answered);
  const answeredCount = Number.isFinite(prevAnswered)
    ? prevAnswered + 1
    : Math.max(0, totalCount - remainingBefore) + 1;
  let correctCount = Number(state.correct || 0);
  let currentItemId = '';
  let currentFavorite = 0;

  try {
    if ((filters.source === 'cards' || filters.source === 'mixed') && String(word_id).startsWith('card_')) {
      const cardId = Number(String(word_id).replace('card_', ''));
      const chosenId = String(choice_word_id).replace('card_', '');
      const card = await get(
        `SELECT c.*, COALESCE(co.favorite, c.favorite) AS effective_favorite
         FROM cards c
         LEFT JOIN card_overrides co ON co.card_id = c.id AND co.user_id = ?
         WHERE c.id = ?`,
        [userId, cardId]
      );
      if (!card) return res.redirect('/train');
      currentItemId = `card_${cardId}`;
      currentFavorite = card.effective_favorite ? 1 : 0;
      const isCorrect = String(chosenId) === String(cardId);
      if (isCorrect) correctCount += 1;
      await upsertCardProgress(userId, cardId, isCorrect);
      if (remainingCount > 0) {
        req.session.trainState = { ...state, correct: correctCount, answered: answeredCount, remaining: remainingCount, total: totalCount, modes: modeList, replay: null };
      } else {
        delete req.session.trainState;
      }
      const result = {
        isCorrect,
        hebrew: card.hebrew,
        transliteration: card.transliteration,
        french: card.french
      };
      const themes = await getActiveThemesForUser(userId);
      return res.render('train', {
        currentItemId,
        currentFavorite,
        word: null,
        message: remainingCount > 0 ? null : 'Session terminee.',
        result,
        mode: modeList[0] || activeMode,
        modes: modeList,
        questionMode: activeMode,
        filters,
        options: null,
        themes,
        remaining: remainingCount,
        total: totalCount,
        answered: answeredCount,
        correct: correctCount,
        nextUrl: remainingCount > 0 ? '/train' : null
      });
    } else {
      const word = await get('SELECT * FROM words WHERE id = ?', [word_id]);
      if (!word) return res.redirect('/train');
      currentItemId = String(word_id);
      const favRow = await get('SELECT id FROM favorites WHERE user_id = ? AND word_id = ?', [userId, word_id]);
      currentFavorite = favRow ? 1 : 0;
      const isCorrect = String(choice_word_id) === String(word_id);
      if (isCorrect) correctCount += 1;
      await upsertProgress(userId, word_id, isCorrect);
      if (remainingCount > 0) {
        req.session.trainState = { ...state, correct: correctCount, answered: answeredCount, remaining: remainingCount, total: totalCount, modes: modeList, replay: null };
      } else {
        delete req.session.trainState;
      }

      const result = {
        isCorrect,
        hebrew: word.hebrew,
        transliteration: word.transliteration,
        french: word.french
      };

      const nextUrl = remainingCount > 0 ? `/train` : null;
      const themes = await getActiveThemesForUser(userId);
      res.render('train', {
        currentItemId,
        currentFavorite,
        word: null,
        message: remainingCount > 0 ? null : 'Session terminee.',
        result,
        mode: modeList[0] || activeMode,
        modes: modeList,
        questionMode: activeMode,
        filters,
        options: null,
        themes,
        remaining: remainingCount,
        total: totalCount,
        answered: answeredCount,
        correct: correctCount,
        nextUrl
      });
    }
  } catch (e) {
    console.error(e);
    res.render('train', {
      currentItemId,
      currentFavorite,
      word: null,
      message: 'Erreur serveur.',
      result: null,
      mode: modeList[0] || activeMode,
      modes: modeList,
      questionMode: activeMode,
      filters,
      options: null,
      themes: [],
      remaining: 0,
      total: 0,
      answered: 0,
      correct: 0,
      nextUrl: null
    });
  }
});

app.get('/my/words/new', requireAuth, async (req, res) => {
  try {
    const userId = req.session.user.id;
    const themes = await all('SELECT * FROM themes WHERE user_id = ? AND active = 1 ORDER BY id ASC', [userId]);
    const levels = await getOwnedLevels(userId);
    res.render('my_word_form', {
      word: null,
      themes,
      levels,
      action: '/my/words'
    });
  } catch (e) {
    console.error(e);
    res.redirect('/my/words');
  }
});

app.post('/my/words', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const { hebrew, transliteration, french, theme_id, level_id, difficulty, active } = req.body;
  try {
    const { levelId: normLevel, themeId: normTheme } = await normalizeLevelSelection(theme_id, level_id);
    let themeId = null;
    if (normTheme) {
      const t = await get('SELECT id FROM themes WHERE id = ? AND user_id = ? AND active = 1', [normTheme, userId]);
      if (t) themeId = t.id;
    }
    let levelId = null;
    if (normLevel) {
      const l = await get(
        `SELECT tl.id, tl.theme_id, tl.active, t.user_id
         FROM theme_levels tl
         JOIN themes t ON t.id = tl.theme_id
         WHERE tl.id = ?`,
        [normLevel]
      );
      if (l && l.active && l.user_id === userId) {
        if (themeId && themeId !== l.theme_id) {
          levelId = null;
        } else {
          levelId = l.id;
          themeId = themeId || l.theme_id;
        }
      }
    }
    const wordId = await findFirstAvailableId('words');
    const position = await nextWordPosition(themeId || null, userId);
    await run(
      `INSERT INTO words (id, hebrew, transliteration, french, theme_id, level_id, difficulty, active, user_id, position)
       VALUES (?,?,?,?,?,?,?,?,?,?)`,
      [
        wordId,
        hebrew,
        transliteration || null,
        french,
        themeId || null,
        levelId,
        difficulty || 1,
        active ? 1 : 0,
        userId,
        position
      ]
    );
    res.redirect('/my/words');
  } catch (e) {
    console.error(e);
    res.redirect('/my/words');
  }
});

app.get('/my/words/:id/edit', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  try {
    const word = await get('SELECT * FROM words WHERE id = ? AND user_id = ?', [req.params.id, userId]);
    if (!word) return res.redirect('/my/words');
    const themes = await all('SELECT * FROM themes WHERE user_id = ? AND active = 1 ORDER BY id ASC', [userId]);
    const levels = await getOwnedLevels(userId);
    res.render('my_word_form', {
      word,
      themes,
      levels,
      action: `/my/words/${word.id}?_method=PUT`
    });
  } catch (e) {
    console.error(e);
    res.redirect('/my/words');
  }
});

app.put('/my/words/:id', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const { hebrew, transliteration, french, theme_id, level_id, difficulty, active } = req.body;
  try {
    const { levelId: normLevel, themeId: normTheme } = await normalizeLevelSelection(theme_id, level_id);
    let themeId = null;
    if (normTheme) {
      const t = await get('SELECT id FROM themes WHERE id = ? AND user_id = ? AND active = 1', [normTheme, userId]);
      if (t) themeId = t.id;
    }
    let levelId = null;
    if (normLevel) {
      const l = await get(
        `SELECT tl.id, tl.theme_id, tl.active, t.user_id
         FROM theme_levels tl
         JOIN themes t ON t.id = tl.theme_id
         WHERE tl.id = ?`,
        [normLevel]
      );
      if (l && l.active && l.user_id === userId) {
        if (themeId && themeId !== l.theme_id) {
          levelId = null;
        } else {
          levelId = l.id;
          themeId = themeId || l.theme_id;
        }
      }
    }
    await run(
      `UPDATE words
       SET hebrew = ?, transliteration = ?, french = ?, theme_id = ?, level_id = ?, difficulty = ?, active = ?
       WHERE id = ? AND user_id = ?`,
      [
        hebrew,
        transliteration || null,
        french,
        themeId || null,
        levelId,
        difficulty || 1,
        active ? 1 : 0,
        req.params.id,
        userId
      ]
    );
    res.redirect('/my/words');
  } catch (e) {
    console.error(e);
    res.redirect('/my/words');
  }
});

app.delete('/my/words/:id', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const redirectTo = req.body.redirectTo;
  const backUrl = redirectTo && redirectTo.startsWith('/') ? redirectTo : '/my/words';
  try {
    await run('DELETE FROM progress WHERE word_id = ? AND user_id = ?', [req.params.id, userId]);
    await run('DELETE FROM favorites WHERE word_id = ? AND user_id = ?', [req.params.id, userId]);
    await run('DELETE FROM words WHERE id = ? AND user_id = ?', [req.params.id, userId]);
    res.redirect(backUrl);
  } catch (e) {
    console.error(e);
    res.redirect(backUrl);
  }
});

// ---------- Mes listes perso ----------
app.get('/my/lists', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  try {
    const sort = req.query.sort || 'created_asc';
    let orderBy = 's.created_at ASC';
    if (sort === 'created_desc') orderBy = 's.created_at DESC';
    if (sort === 'alpha') orderBy = 's.name COLLATE NOCASE ASC';
    const owned = await all(
      `SELECT s.*,
        COUNT(c.id) AS card_count,
        COALESCE(uso.active, s.active) AS effective_active,
        GROUP_CONCAT(DISTINCT CASE WHEN sh.can_edit = 1 THEN u.display_name END) AS collaborators,
        GROUP_CONCAT(DISTINCT CASE WHEN (sh.can_edit IS NULL OR sh.can_edit = 0) THEN u.display_name END) AS shared_with
       FROM sets s
       LEFT JOIN cards c ON c.set_id = s.id AND c.active = 1
       LEFT JOIN set_shares sh ON sh.set_id = s.id
       LEFT JOIN users u ON u.id = sh.user_id
       LEFT JOIN user_set_overrides uso ON uso.set_id = s.id AND uso.user_id = ?
       WHERE s.user_id = ? AND s.active = 1
       GROUP BY s.id
       ORDER BY ${orderBy}`,
      [userId, userId]
    );
    const shared = await all(
      `SELECT s.*, owner.display_name AS owner_name,
        COUNT(c.id) AS card_count,
        COALESCE(uso.active, s.active) AS effective_active,
        sh.can_edit
       FROM set_shares sh
       JOIN sets s ON s.id = sh.set_id
       JOIN users owner ON owner.id = s.user_id
       LEFT JOIN cards c ON c.set_id = s.id AND c.active = 1
       LEFT JOIN user_set_overrides uso ON uso.set_id = s.id AND uso.user_id = ?
       WHERE sh.user_id = ? AND s.active = 1
       GROUP BY s.id
       ORDER BY ${orderBy}`,
      [userId, userId]
    );
    res.render('my_lists', { sets: owned, sharedSets: shared, pageClass: 'page-compact', sort });
  } catch (e) {
    console.error(e);
    res.render('my_lists', { sets: [], sharedSets: [], pageClass: 'page-compact', sort: req.query.sort || 'created_asc' });
  }
});

app.post('/my/lists/:id/toggle-active', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const backUrl = req.get('referer') || '/my/lists';
  try {
    const set = await get(
      `SELECT s.*, COALESCE(uso.active, s.active) AS effective_active, sh.id AS shared_id
       FROM sets s
       LEFT JOIN user_set_overrides uso ON uso.set_id = s.id AND uso.user_id = ?
       LEFT JOIN set_shares sh ON sh.set_id = s.id AND sh.user_id = ?
       WHERE s.id = ?`,
      [userId, userId, req.params.id]
    );
    if (!set) return res.redirect('/my/lists');
    const isOwner = set.user_id === userId;
    const isShared = !!set.shared_id;
    if (!isOwner && !isShared) return res.redirect('/my/lists');
    if (!set.active) return res.redirect(backUrl);
    const nextStatus = set.effective_active ? 0 : 1;
    await run(
      'INSERT INTO user_set_overrides (set_id, user_id, active) VALUES (?,?,?) ON CONFLICT(set_id, user_id) DO UPDATE SET active=excluded.active',
      [set.id, userId, nextStatus]
    );
  } catch (e) {
    console.error('Toggle list active error:', e);
  }
  res.redirect(backUrl);
});

app.post('/my/lists/:id/leave', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const backUrl = req.get('referer') || '/my/lists';
  try {
    const shared = await get('SELECT 1 FROM set_shares WHERE set_id = ? AND user_id = ?', [req.params.id, userId]);
    if (!shared) return res.redirect(backUrl);
    await run('DELETE FROM set_shares WHERE set_id = ? AND user_id = ?', [req.params.id, userId]);
    await run('DELETE FROM user_set_overrides WHERE set_id = ? AND user_id = ?', [req.params.id, userId]);
    await run(
      `DELETE FROM card_overrides WHERE user_id = ? AND card_id IN (SELECT id FROM cards WHERE set_id = ?)`,
      [userId, req.params.id]
    );
  } catch (e) {
    console.error('Leave shared list error:', e);
  }
  res.redirect('/my/lists');
});

app.get('/space', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  try {
    const overallWords = await get(
      `SELECT
        COUNT(DISTINCT p.word_id) AS words_seen,
        SUM(p.success_count) AS total_success,
        SUM(p.fail_count) AS total_fail,
        AVG(p.strength) AS avg_strength,
        COUNT(*) AS progress_count
       FROM progress p
       WHERE p.user_id = ?`,
      [userId]
    );
    const overallCards = await get(
      `SELECT
        COUNT(DISTINCT cp.card_id) AS cards_seen,
        SUM(cp.success_count) AS total_success,
        SUM(cp.fail_count) AS total_fail
       FROM card_progress cp
       JOIN cards c ON c.id = cp.card_id
       JOIN sets s ON s.id = c.set_id AND s.active = 1
       LEFT JOIN user_set_overrides uso ON uso.set_id = s.id AND uso.user_id = ?
       LEFT JOIN set_shares sh ON sh.set_id = s.id AND sh.user_id = ?
       WHERE cp.user_id = ? AND COALESCE(uso.active, s.active) = 1 AND (s.user_id = ? OR sh.id IS NOT NULL)`,
      [userId, userId, userId, userId]
    );
    const overall = {
      words_seen: overallWords?.words_seen || 0,
      cards_seen: overallCards?.cards_seen || 0,
      total_success: (overallWords?.total_success || 0) + (overallCards?.total_success || 0),
      total_fail: (overallWords?.total_fail || 0) + (overallCards?.total_fail || 0),
      avg_strength: 0
    };
    overall.total_seen = overall.words_seen + overall.cards_seen;
    const allAttempts = overall.total_success + overall.total_fail;
    if (allAttempts > 0) {
      overall.avg_strength = Math.round((overall.total_success / allAttempts) * 100);
    }

    const setStats = await all(
      `SELECT s.id, s.name, owner.display_name AS owner_name,
              COUNT(c.id) AS total_cards,
              SUM(CASE WHEN c.active = 1 THEN 1 ELSE 0 END) AS active_cards,
              SUM(CASE WHEN c.memorized = 1 THEN 1 ELSE 0 END) AS memorized_cards,
              SUM(CASE WHEN c.favorite = 1 THEN 1 ELSE 0 END) AS favorite_cards
       FROM sets s
       JOIN users owner ON owner.id = s.user_id
       LEFT JOIN cards c ON c.set_id = s.id
       LEFT JOIN set_shares sh ON sh.set_id = s.id AND sh.user_id = ?
       LEFT JOIN user_set_overrides uso ON uso.set_id = s.id AND uso.user_id = ?
       WHERE s.active = 1 AND COALESCE(uso.active, s.active) = 1 AND (s.user_id = ? OR sh.id IS NOT NULL)
       GROUP BY s.id
       ORDER BY s.created_at DESC`,
      [userId, userId, userId]
    );

    const themeStats = await all(
      `SELECT t.id, t.name,
              COUNT(w.id) AS total_words,
              SUM(CASE WHEN p.id IS NOT NULL THEN 1 ELSE 0 END) AS seen_words,
              AVG(p.strength) AS avg_strength
       FROM themes t
       LEFT JOIN words w ON w.theme_id = t.id AND w.active = 1
       LEFT JOIN progress p ON p.word_id = w.id AND p.user_id = ?
       LEFT JOIN user_theme_overrides uto ON uto.theme_id = t.id AND uto.user_id = ?
       WHERE t.user_id IS NULL
         AND t.active = 1
         AND COALESCE(uto.active, 1) = 1
         AND NOT EXISTS (
           SELECT 1 FROM themes child
           WHERE child.parent_id = t.id AND child.active = 1
         )
       GROUP BY t.id
       ORDER BY t.id ASC`,
      [userId, userId]
    );

    res.render('space', {
      overall,
      setStats,
      themeStats,
      pageClass: 'page-compact'
    });
  } catch (e) {
    console.error(e);
    res.render('space', {
      overall: null,
      setStats: [],
      themeStats: [],
      pageClass: 'page-compact'
    });
  }
});

// ---------- Recherche globale ----------
app.get('/search', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  try {
    const themeWords = await all(
      `SELECT w.id, w.hebrew, w.french, w.transliteration,
              w.active AS word_active,
              w.user_id,
              w.theme_id,
              w.level_id,
              t.name AS theme_name,
              t.user_id AS theme_owner_id,
              t.active AS theme_active,
              COALESCE(uto.active, t.active) AS theme_user_active,
              l.name AS level_name,
              l.active AS level_active,
              uwo.active AS override_active
         FROM words w
         LEFT JOIN themes t ON t.id = w.theme_id
         LEFT JOIN theme_levels l ON l.id = w.level_id
         LEFT JOIN user_theme_overrides uto ON uto.theme_id = w.theme_id AND uto.user_id = ?
         LEFT JOIN user_word_overrides uwo ON uwo.word_id = w.id AND uwo.user_id = ?
        WHERE (w.user_id IS NULL OR w.user_id = ?)
          AND (w.theme_id IS NULL OR t.user_id IS NULL OR t.user_id = ?)
        ORDER BY w.id ASC`,
      [userId, userId, userId, userId]
    );

    const listCards = await all(
      `SELECT c.id, c.hebrew, c.french, c.transliteration,
              c.active AS card_active,
              c.set_id,
              s.name AS set_name,
              s.user_id AS owner_id,
              owner.display_name AS owner_name,
              s.active AS set_active,
              COALESCE(uso.active, s.active) AS set_user_active,
              co.active AS override_active,
              sh.id AS share_id
         FROM sets s
         JOIN users owner ON owner.id = s.user_id
         JOIN cards c ON c.set_id = s.id
         LEFT JOIN set_shares sh ON sh.set_id = s.id AND sh.user_id = ?
         LEFT JOIN user_set_overrides uso ON uso.set_id = s.id AND uso.user_id = ?
         LEFT JOIN card_overrides co ON co.card_id = c.id AND co.user_id = ?
        WHERE s.user_id = ? OR sh.id IS NOT NULL
        ORDER BY s.created_at DESC, c.position ASC, c.id ASC`,
      [userId, userId, userId, userId]
    );

    const searchItems = [];

    for (const w of themeWords) {
      const themeActive = w.theme_id ? (w.theme_user_active === null ? w.theme_active : w.theme_user_active) : 1;
      const levelActive = w.level_id ? w.level_active : 1;
      const containerActive = themeActive && levelActive ? 1 : 0;
      const effectiveActive =
        (w.word_active && containerActive)
          ? (w.override_active === null ? w.word_active : w.override_active)
          : 0;
      searchItems.push({
        id: `word-${w.id}`,
        kind: 'theme',
        hebrew: w.hebrew,
        french: w.french,
        transliteration: w.transliteration || '',
        location: w.theme_name || 'Sans theme',
        level: w.level_name || '',
        origin: w.user_id ? 'mine' : 'global',
        active: effectiveActive ? 1 : 0,
        baseActive: w.word_active ? 1 : 0,
        containerActive,
        levelActive: levelActive ? 1 : 0
      });
    }

    for (const c of listCards) {
      const setActive = c.set_user_active === null ? c.set_active : c.set_user_active;
      const cardActive = c.override_active === null ? c.card_active : c.override_active;
      const effectiveActive = setActive && cardActive ? 1 : 0;
      searchItems.push({
        id: `card-${c.id}`,
        kind: 'list',
        hebrew: c.hebrew,
        french: c.french,
        transliteration: c.transliteration || '',
        location: c.set_name,
        owner: c.owner_name,
        origin: c.owner_id === userId ? 'mine' : 'shared',
        active: effectiveActive ? 1 : 0,
        baseActive: c.card_active ? 1 : 0,
        containerActive: setActive ? 1 : 0,
        levelActive: 1
      });
    }

    searchItems.sort((a, b) => {
      return String(a.french || '').localeCompare(String(b.french || ''), 'fr', { sensitivity: 'base' });
    });

    res.render('search', {
      searchItems,
      counts: {
        total: searchItems.length,
        themes: themeWords.length,
        lists: listCards.length
      },
      title: 'Recherche'
    });
  } catch (e) {
    console.error('Search page error:', e);
    res.render('search', { searchItems: [], counts: { total: 0, themes: 0, lists: 0 }, title: 'Recherche' });
  }
});

app.get('/search/duplicates', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const isAdmin = req.session.user && req.session.user.role === 'admin';
  try {
    const ignoredRows = await all('SELECT dup_key FROM duplicate_ignores WHERE user_id = ?', [userId]);
    const ignoredSet = new Set(ignoredRows.map(r => r.dup_key));

    const themeWords = await all(
      `SELECT w.id, w.hebrew, w.french, w.transliteration, w.user_id, w.theme_id, w.level_id,
              w.active AS word_active,
              t.name AS theme_name,
              t.active AS theme_active,
              COALESCE(uto.active, t.active) AS theme_user_active,
              l.name AS level_name,
              l.active AS level_active,
              uwo.active AS override_active
         FROM words w
         LEFT JOIN themes t ON t.id = w.theme_id
         LEFT JOIN theme_levels l ON l.id = w.level_id
         LEFT JOIN user_theme_overrides uto ON uto.theme_id = w.theme_id AND uto.user_id = ?
         LEFT JOIN user_word_overrides uwo ON uwo.word_id = w.id AND uwo.user_id = ?
        WHERE w.user_id IS NULL OR w.user_id = ?`,
      [userId, userId, userId]
    );

    const accessibleSetIds = await getAccessibleSetIds(userId);
    let listCards = [];
    if (accessibleSetIds.length > 0) {
      const ph = accessibleSetIds.map(() => '?').join(',');
      listCards = await all(
        `SELECT c.id, c.hebrew, c.french, c.transliteration,
                c.active AS card_active,
                c.set_id,
                s.name AS set_name,
                s.user_id AS owner_id,
                u.display_name AS owner_name,
                s.active AS set_active,
                COALESCE(uso.active, s.active) AS set_user_active,
                co.active AS override_active,
                sh.can_edit
           FROM cards c
           JOIN sets s ON s.id = c.set_id
           JOIN users u ON u.id = s.user_id
           LEFT JOIN set_shares sh ON sh.set_id = s.id AND sh.user_id = ?
           LEFT JOIN user_set_overrides uso ON uso.set_id = s.id AND uso.user_id = ?
           LEFT JOIN card_overrides co ON co.card_id = c.id AND co.user_id = ?
          WHERE s.id IN (${ph})`,
        [userId, userId, userId, ...accessibleSetIds]
      );
    }

    const map = new Map();
    const addItem = (key, item) => {
      if (!key) return;
      if (!map.has(key)) map.set(key, []);
      map.get(key).push(item);
    };

    themeWords.forEach(w => {
      const key = duplicateKey(w.hebrew, w.french);
      const themeActive = w.theme_id ? (w.theme_user_active === null ? w.theme_active : w.theme_user_active) : 1;
      const levelActive = w.level_id ? w.level_active : 1;
      const containerActive = themeActive && levelActive ? 1 : 0;
      const effectiveActive = containerActive ? (w.override_active === null ? w.word_active : w.override_active) : 0;
      addItem(key, {
        key,
        kind: 'theme',
        id: w.id,
        hebrew: w.hebrew,
        french: w.french,
        transliteration: w.transliteration || '',
        container: w.theme_name || 'Sans theme',
        level: w.level_name || '',
        themeId: w.theme_id || null,
        active: effectiveActive ? 1 : 0,
        baseActive: w.word_active ? 1 : 0,
        containerActive,
        levelActive: levelActive ? 1 : 0,
        isOwner: w.user_id === userId,
        isGlobal: !w.user_id
      });
    });

    listCards.forEach(c => {
      const key = duplicateKey(c.hebrew, c.french);
      const setActive = c.set_user_active === null ? c.set_active : c.set_user_active;
      const cardActive = c.override_active === null ? c.card_active : c.override_active;
      const effectiveActive = setActive && cardActive ? 1 : 0;
      const isOwner = c.owner_id === userId;
      const canEdit = isOwner || Number(c.can_edit) === 1;
      addItem(key, {
        key,
        kind: 'list',
        id: c.id,
        setId: c.set_id,
        hebrew: c.hebrew,
        french: c.french,
        transliteration: c.transliteration || '',
        container: c.set_name || 'Sans liste',
        ownerName: c.owner_name || '',
        active: effectiveActive ? 1 : 0,
        baseActive: c.card_active ? 1 : 0,
        containerActive: setActive ? 1 : 0,
        isOwner,
        canEdit
      });
    });

    const groups = Array.from(map.entries())
      .filter(([, items]) => items.length > 1)
      .map(([key, items]) => ({
        key,
        items,
        sample: items[0],
        kind:
          items.some(i => i.kind === 'theme') && items.some(i => i.kind === 'list')
            ? 'cross'
            : items[0].kind
      }))
      .sort((a, b) => String(a.sample.french || '').localeCompare(String(b.sample.french || ''), 'fr', { sensitivity: 'base' }));

    const ignoredGroups = groups.filter(g => ignoredSet.has(g.key));
    const activeGroups = groups.filter(g => !ignoredSet.has(g.key));

    res.render('duplicates_user', { groups: activeGroups, ignoredGroups, title: 'Doublons', isAdmin });
  } catch (e) {
    console.error('User duplicate scan error:', e);
    res.render('duplicates_user', { groups: [], ignoredGroups: [], title: 'Doublons', isAdmin: false });
  }
});

app.post('/search/duplicates/ignore', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const { dup_key, action } = req.body;
  const backUrl = '/search/duplicates';
  if (!dup_key) return res.redirect(backUrl);
  try {
    if (action === 'ignore') {
      await run('INSERT OR IGNORE INTO duplicate_ignores (user_id, dup_key) VALUES (?, ?)', [userId, dup_key]);
    } else if (action === 'unignore') {
      await run('DELETE FROM duplicate_ignores WHERE user_id = ? AND dup_key = ?', [userId, dup_key]);
    }
  } catch (e) {
    console.error('Ignore duplicate toggle error:', e);
  }
  res.redirect(backUrl);
});

app.get('/my/lists/new', requireAuth, (req, res) => {
  res.render('my_list_form', {
    set: null,
    cards: [],
    action: '/my/lists',
    error: null
  });
});

app.get('/my/lists/:id', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  try {
    const set = await get(
      `SELECT s.*, COALESCE(uso.active, s.active) AS effective_active
       FROM sets s
       LEFT JOIN user_set_overrides uso ON uso.set_id = s.id AND uso.user_id = ?
       WHERE s.id = ?`,
      [userId, req.params.id]
    );
    if (!set) return res.redirect('/my/lists');
    const shared = await get('SELECT can_edit FROM set_shares WHERE set_id = ? AND user_id = ?', [set.id, userId]);
    const isOwner = set.user_id === userId;
    const isCollaborator = shared && Number(shared.can_edit) === 1;
    if (!isOwner && !shared) return res.redirect('/my/lists');
    const cards = await all(
      `SELECT c.*,
        COALESCE(co.active, c.active) AS effective_active,
        COALESCE(co.favorite, c.favorite) AS effective_favorite,
        COALESCE(co.memorized, c.memorized) AS effective_memorized
       FROM cards c
       LEFT JOIN card_overrides co ON co.card_id = c.id AND co.user_id = ?
       WHERE c.set_id = ?
       ORDER BY c.position ASC, c.id ASC`,
      [userId, set.id]
    );
    const normalizedCards = cards.map(c => ({
      ...c,
      active: c.effective_active,
      favorite: c.effective_favorite,
      memorized: c.effective_memorized
    }));
    const otherSets = await all(
      'SELECT id, name FROM sets WHERE user_id = ? AND id != ? AND active = 1 ORDER BY created_at DESC',
      [userId, set.id]
    );
    res.render('my_list_show', {
      set,
      cards: normalizedCards,
      otherSets,
      isOwner,
      isCollaborator
    });
  } catch (e) {
    console.error(e);
    res.redirect('/my/lists');
  }
});

app.post('/my/lists/:id/move', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const sourceId = Number(req.params.id);
  const targetId = Number(req.body.target_set_id);
  const cardIds = normalizeIds(req.body.card_ids);
  if (!targetId || cardIds.length === 0) return res.redirect(`/my/lists/${sourceId}`);
  try {
    const source = await get('SELECT * FROM sets WHERE id = ? AND user_id = ?', [sourceId, userId]);
    if (!source) return res.redirect('/my/lists');
    const target = await get('SELECT * FROM sets WHERE id = ? AND user_id = ?', [targetId, userId]);
    if (!target || target.id === source.id) return res.redirect(`/my/lists/${source.id}`);

    const placeholders = cardIds.map(() => '?').join(',');
    const owned = await all(
      `SELECT id FROM cards WHERE id IN (${placeholders}) AND set_id = ?`,
      [...cardIds, source.id]
    );
    if (owned.length === 0) return res.redirect(`/my/lists/${source.id}`);

    const posRow = await get('SELECT MAX(position) AS pos FROM cards WHERE set_id = ?', [target.id]);
    let nextPos = posRow && posRow.pos ? Number(posRow.pos) : 0;

    const ownedSet = new Set(owned.map(c => c.id));
    for (const cardId of cardIds) {
      if (!ownedSet.has(cardId)) continue;
      nextPos += 1;
      await run('UPDATE cards SET set_id = ?, position = ? WHERE id = ?', [target.id, nextPos, cardId]);
    }

    const remaining = await all('SELECT id FROM cards WHERE set_id = ? ORDER BY position ASC, id ASC', [source.id]);
    for (let i = 0; i < remaining.length; i++) {
      await run('UPDATE cards SET position = ? WHERE id = ?', [i + 1, remaining[i].id]);
    }

    res.redirect(`/my/lists/${target.id}`);
  } catch (e) {
    console.error('Move cards error:', e);
    res.redirect(`/my/lists/${sourceId}`);
  }
});

app.post('/my/lists/:id/copy', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const sourceId = Number(req.params.id);
  const targetId = Number(req.body.target_set_id);
  const cardIds = normalizeIds(req.body.card_ids);
  if (!targetId || cardIds.length === 0) return res.redirect(`/my/lists/${sourceId}`);
  try {
    const source = await get('SELECT * FROM sets WHERE id = ? AND user_id = ?', [sourceId, userId]);
    if (!source) return res.redirect('/my/lists');
    const target = await get('SELECT * FROM sets WHERE id = ? AND user_id = ?', [targetId, userId]);
    if (!target) return res.redirect(`/my/lists/${source.id}`);

    const placeholders = cardIds.map(() => '?').join(',');
    const owned = await all(
      `SELECT * FROM cards WHERE id IN (${placeholders}) AND set_id = ?`,
      [...cardIds, source.id]
    );
    if (owned.length === 0) return res.redirect(`/my/lists/${source.id}`);

    const posRow = await get('SELECT MAX(position) AS pos FROM cards WHERE set_id = ?', [target.id]);
    let nextPos = posRow && posRow.pos ? Number(posRow.pos) : 0;

    for (const card of owned) {
      nextPos += 1;
      await run(
        `INSERT INTO cards (set_id, french, hebrew, transliteration, position, active, favorite, memorized)
         VALUES (?,?,?,?,?,?,?,?)`,
        [
          target.id,
          card.french || '',
          card.hebrew || '',
          card.transliteration || null,
          nextPos,
          card.active ? 1 : 0,
          card.favorite ? 1 : 0,
          card.memorized ? 1 : 0
        ]
      );
    }

    res.redirect(`/my/lists/${target.id}`);
  } catch (e) {
    console.error('Copy cards error:', e);
    res.redirect(`/my/lists/${sourceId}`);
  }
});

app.get('/my/lists/:id/cards/:cardId/edit', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  try {
    const set = await get('SELECT * FROM sets WHERE id = ?', [req.params.id]);
    if (!set) return res.redirect('/my/lists');
    const shared = await get('SELECT can_edit FROM set_shares WHERE set_id = ? AND user_id = ?', [set.id, userId]);
    const canEdit = set.user_id === userId || (shared && Number(shared.can_edit) === 1);
    if (!canEdit) return res.redirect('/my/lists');
    const card = await get('SELECT * FROM cards WHERE id = ? AND set_id = ?', [req.params.cardId, set.id]);
    if (!card) return res.redirect(`/my/lists/${set.id}`);
    res.render('my_card_form', {
      set,
      card,
      action: `/my/lists/${set.id}/cards/${card.id}?_method=PUT`,
      error: null
    });
  } catch (e) {
    console.error(e);
    res.redirect('/my/lists');
  }
});

app.put('/my/lists/:id/cards/:cardId', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  try {
    const set = await get('SELECT * FROM sets WHERE id = ?', [req.params.id]);
    if (!set) return res.redirect('/my/lists');
    const shared = await get('SELECT can_edit FROM set_shares WHERE set_id = ? AND user_id = ?', [set.id, userId]);
    const canEdit = set.user_id === userId || (shared && Number(shared.can_edit) === 1);
    if (!canEdit) return res.redirect('/my/lists');
    const card = await get('SELECT * FROM cards WHERE id = ? AND set_id = ?', [req.params.cardId, set.id]);
    if (!card) return res.redirect(`/my/lists/${set.id}`);
    const { hebrew, french, transliteration, active, favorite, memorized } = req.body;
    if (!hebrew || !french) {
      return res.render('my_card_form', {
        set,
        card: { ...card, hebrew, french, transliteration, active: active ? 1 : 0, favorite: favorite ? 1 : 0, memorized: memorized ? 1 : 0 },
        action: `/my/lists/${set.id}/cards/${card.id}?_method=PUT`,
        error: 'Hébreu et français sont obligatoires.'
      });
    }
    await run(
      `UPDATE cards
       SET hebrew = ?, french = ?, transliteration = ?, active = ?, favorite = ?, memorized = ?
       WHERE id = ? AND set_id = ?`,
      [hebrew, french, transliteration || null, active ? 1 : 0, favorite ? 1 : 0, memorized ? 1 : 0, card.id, set.id]
    );
    res.redirect(`/my/lists/${set.id}`);
  } catch (e) {
    console.error(e);
    res.redirect('/my/lists');
  }
});

app.get('/my/lists/:id/edit', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  try {
    const set = await get('SELECT * FROM sets WHERE id = ?', [req.params.id]);
    if (!set) return res.redirect('/my/lists');
    const shared = await get('SELECT can_edit FROM set_shares WHERE set_id = ? AND user_id = ?', [set.id, userId]);
    const canEdit = set.user_id === userId || (shared && Number(shared.can_edit) === 1);
    if (!canEdit) return res.redirect('/my/lists');
    const cards = await all(
      'SELECT * FROM cards WHERE set_id = ? ORDER BY position ASC, id ASC',
      [set.id]
    );
    res.render('my_list_form', {
      set,
      cards,
      action: `/my/lists/${set.id}?_method=PUT`,
      error: null
    });
  } catch (e) {
    console.error(e);
    res.redirect('/my/lists');
  }
});

app.get('/my/lists/:id/share', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  try {
    const set = await get('SELECT * FROM sets WHERE id = ? AND user_id = ?', [req.params.id, userId]);
    if (!set) return res.redirect('/my/lists');
    const users = await all('SELECT id, display_name, email FROM users WHERE role = ? AND id != ? ORDER BY display_name ASC', ['user', userId]);
    const sharedRows = await all('SELECT user_id, can_edit FROM set_shares WHERE set_id = ?', [set.id]);
    const currentShared = sharedRows.filter(r => Number(r.can_edit) === 0).map(r => Number(r.user_id));
    const currentCollaborators = sharedRows.filter(r => Number(r.can_edit) === 1).map(r => Number(r.user_id));
    res.render('my_list_share', { set, users, currentShared, currentCollaborators });
  } catch (e) {
    console.error(e);
    res.redirect('/my/lists');
  }
});

app.post('/my/lists/:id/share', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const targetIds = normalizeIds(req.body.user_ids);
  const collaboratorIds = normalizeIds(req.body.collaborator_ids);
  try {
    const set = await get('SELECT * FROM sets WHERE id = ? AND user_id = ?', [req.params.id, userId]);
    if (!set) return res.redirect('/my/lists');
    await run('DELETE FROM set_shares WHERE set_id = ?', [set.id]);
    const collabSet = new Set(collaboratorIds);
    const shareSet = new Set(targetIds);
    const allTargets = new Set([...shareSet, ...collabSet]);
    for (const uid of allTargets) {
      const canEdit = collabSet.has(uid) ? 1 : 0;
      await run('INSERT OR IGNORE INTO set_shares (set_id, user_id, can_edit) VALUES (?,?,?)', [set.id, uid, canEdit]);
      await run('UPDATE set_shares SET can_edit = ? WHERE set_id = ? AND user_id = ?', [canEdit, set.id, uid]);
    }
    res.redirect('/my/lists');
  } catch (e) {
    console.error(e);
    res.redirect('/my/lists');
  }
});

app.post('/my/lists/:id/duplicate', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  try {
    const source = await get('SELECT * FROM sets WHERE id = ? AND user_id = ?', [req.params.id, userId]);
    if (!source) return res.redirect('/my/lists');
    let suffix = 1;
    let newName = `${source.name} (${suffix})`;
    // Avoid name collisions similar to Windows
    // Keep incrementing suffix until an available name is found
    // Add small guard to avoid tight infinite loop
    while (suffix < 200) {
      const existing = await get('SELECT 1 FROM sets WHERE user_id = ? AND name = ?', [userId, newName]);
      if (!existing) break;
      suffix += 1;
      newName = `${source.name} (${suffix})`;
    }
    const displayNo = await nextDisplayNo('sets', userId);
    const newId = await findFirstAvailableId('sets');
    const info = await run('INSERT INTO sets (id, name, user_id, active, display_no) VALUES (?,?,?,1,?)', [newId, newName, userId, displayNo]);
    const cards = await all('SELECT * FROM cards WHERE set_id = ? ORDER BY position ASC, id ASC', [source.id]);
    for (const card of cards) {
      await run(
        `INSERT INTO cards (set_id, french, hebrew, transliteration, position, active, favorite, memorized)
         VALUES (?,?,?,?,?,?,?,?)`,
        [
          info.lastID,
          card.french || '',
          card.hebrew || '',
          card.transliteration || null,
          card.position || 1,
          card.active ? 1 : 0,
          card.favorite ? 1 : 0,
          card.memorized ? 1 : 0
        ]
      );
    }
    res.redirect('/my/lists');
  } catch (e) {
    console.error('Duplicate list error:', e);
    res.redirect('/my/lists');
  }
});

app.post('/my/lists', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const { name } = req.body;
  const cards = normalizeCardsPayload(req.body.cards);
  const title = name ? name.trim() : '';
  if (!title) {
    return res.render('my_list_form', {
      set: null,
      cards,
      action: '/my/lists',
      error: 'Donne un titre a ta liste.'
    });
  }
  try {
    const displayNo = await nextDisplayNo('sets', userId);
    const newId = await findFirstAvailableId('sets');
    const info = await run(
      'INSERT INTO sets (id, name, user_id, active, display_no) VALUES (?,?,?,1,?)',
      [newId, title, userId, displayNo]
    );
    for (let i = 0; i < cards.length; i++) {
      const card = cards[i];
      await run(
        `INSERT INTO cards (set_id, french, hebrew, transliteration, position, active)
         VALUES (?,?,?,?,?,1)`,
        [info.lastID, card.french || '', card.hebrew || '', card.transliteration || null, i + 1]
      );
    }
    res.redirect('/my/lists');
  } catch (e) {
    console.error(e);
    res.render('my_list_form', {
      set: null,
      cards,
      action: '/my/lists',
      error: 'Impossible de créer la liste pour le moment.'
    });
  }
});

app.put('/my/lists/:id', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const { name } = req.body;
  const cards = normalizeCardsPayload(req.body.cards);
  try {
    const set = await get('SELECT * FROM sets WHERE id = ?', [req.params.id]);
    if (!set) return res.redirect('/my/lists');
    const shared = await get('SELECT can_edit FROM set_shares WHERE set_id = ? AND user_id = ?', [set.id, userId]);
    const isOwner = set.user_id === userId;
    const isCollaborator = shared && Number(shared.can_edit) === 1;
    if (!isOwner && !isCollaborator) return res.redirect('/my/lists');
    const title = name && name.trim() ? name.trim() : set.name;
    await run('UPDATE sets SET name = ? WHERE id = ?', [title, set.id]);

    const existing = await all('SELECT * FROM cards WHERE set_id = ?', [set.id]);
    const existingById = new Map(existing.map(c => [Number(c.id), c]));
    const existingIds = new Set(existingById.keys());
    const keptIds = new Set();

    for (let i = 0; i < cards.length; i++) {
      const card = cards[i];
      const position = i + 1;
      if (card.id && existingIds.has(Number(card.id))) {
        const current = existingById.get(Number(card.id)) || {};
        await run(
          `UPDATE cards
           SET french = ?, hebrew = ?, transliteration = ?, position = ?, active = ?, favorite = ?, memorized = ?
           WHERE id = ? AND set_id = ?`,
          [
            card.french || '',
            card.hebrew || '',
            card.transliteration || null,
            position,
            current.active ?? 1,
            current.favorite ?? 0,
            current.memorized ?? 0,
            card.id,
            set.id
          ]
        );
        keptIds.add(Number(card.id));
      } else {
        const created = await run(
          `INSERT INTO cards (set_id, french, hebrew, transliteration, position, active, favorite, memorized)
           VALUES (?,?,?,?,?,1,0,0)`,
          [set.id, card.french || '', card.hebrew || '', card.transliteration || null, position]
        );
        keptIds.add(created.lastID);
      }
    }

    if (isOwner) {
      for (const id of existingIds) {
        if (!keptIds.has(Number(id))) {
          await run('DELETE FROM cards WHERE id = ? AND set_id = ?', [id, set.id]);
        }
      }
    }

    res.redirect('/my/lists');
  } catch (e) {
    console.error(e);
    res.render('my_list_form', {
      set: { id: req.params.id, name: name || '' },
      cards,
      action: `/my/lists/${req.params.id}?_method=PUT`,
      error: 'Impossible d\'enregistrer la liste.'
    });
  }
});


app.post('/my/lists/:id/cards/:cardId/action', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const isAdmin = req.session.user && req.session.user.role === 'admin';
  const { action, redirectTo } = req.body;
  const fallback = `/my/lists/${req.params.id}`;
  const allowedRedirects = ['/my/lists', '/my/words', '/search/duplicates'];
  const backUrl = redirectTo && allowedRedirects.some(p => redirectTo.startsWith(p)) ? redirectTo : fallback;
  try {
    const set = await get('SELECT * FROM sets WHERE id = ?', [req.params.id]);
    if (!set) return res.redirect('/my/lists');
    let isOwner = set.user_id === userId;
    const shared = await get('SELECT can_edit FROM set_shares WHERE set_id = ? AND user_id = ?', [set.id, userId]);
    const isCollaborator = shared && Number(shared.can_edit) === 1;
    if (isAdmin) {
      isOwner = true; // admin can agir comme proprio pour modifs
    }
    if (!isOwner && !shared && !isAdmin) return res.redirect(backUrl);
    const card = await get('SELECT * FROM cards WHERE id = ? AND set_id = ?', [req.params.cardId, set.id]);
    if (!card) return res.redirect(backUrl);
    const existingOverride = await get('SELECT * FROM card_overrides WHERE card_id = ? AND user_id = ?', [card.id, userId]);

    if (isOwner) {
      if (action === 'toggle_fav') {
        await run('UPDATE cards SET favorite = ? WHERE id = ?', [card.favorite ? 0 : 1, card.id]);
      } else if (action === 'toggle_active') {
        await run('UPDATE cards SET active = ? WHERE id = ?', [card.active ? 0 : 1, card.id]);
      } else if (action === 'toggle_memorized') {
        await run('UPDATE cards SET memorized = ? WHERE id = ?', [card.memorized ? 0 : 1, card.id]);
      } else if (action === 'delete') {
        await run('DELETE FROM cards WHERE id = ? AND set_id = ?', [card.id, set.id]);
        const remaining = await all('SELECT id FROM cards WHERE set_id = ? ORDER BY position ASC, id ASC', [set.id]);
        for (let i = 0; i < remaining.length; i++) {
          await run('UPDATE cards SET position = ? WHERE id = ?', [i + 1, remaining[i].id]);
        }
      }
    } else if (isCollaborator) {
      if (action === 'toggle_fav') {
        const currentFav = existingOverride
          ? (existingOverride.favorite === null ? card.favorite : existingOverride.favorite)
          : card.favorite;
        await run(
          'INSERT OR REPLACE INTO card_overrides (card_id, user_id, active, favorite, memorized) VALUES (?,?,?,?,?)',
          [
            card.id,
            userId,
            existingOverride && existingOverride.active !== null ? existingOverride.active : card.active,
            currentFav ? 0 : 1,
            existingOverride && existingOverride.memorized !== null ? existingOverride.memorized : card.memorized
          ]
        );
      } else if (action === 'toggle_active') {
        await run('UPDATE cards SET active = ? WHERE id = ?', [card.active ? 0 : 1, card.id]);
      } else if (action === 'toggle_memorized') {
        await run('UPDATE cards SET memorized = ? WHERE id = ?', [card.memorized ? 0 : 1, card.id]);
      }
    } else {
      // Shared list: only overrides per user
      const base = {
        active: card.active,
        favorite: card.favorite,
        memorized: card.memorized
      };
      if (action === 'toggle_fav' || action === 'toggle_active' || action === 'toggle_memorized') {
        const next = {
          active: action === 'toggle_active' ? (existingOverride ? (existingOverride.active === null ? base.active : existingOverride.active) : base.active) ? 0 : 1 : (existingOverride ? existingOverride.active : null),
          favorite: action === 'toggle_fav' ? (existingOverride ? (existingOverride.favorite === null ? base.favorite : existingOverride.favorite) : base.favorite) ? 0 : 1 : (existingOverride ? existingOverride.favorite : null),
          memorized: action === 'toggle_memorized' ? (existingOverride ? (existingOverride.memorized === null ? base.memorized : existingOverride.memorized) : base.memorized) ? 0 : 1 : (existingOverride ? existingOverride.memorized : null)
        };
        await run(
          'INSERT OR REPLACE INTO card_overrides (card_id, user_id, active, favorite, memorized) VALUES (?,?,?,?,?)',
          [card.id, userId, next.active, next.favorite, next.memorized]
        );
      }
    }
  } catch (e) {
    console.error(e);
  }
  res.redirect(backUrl);
});

app.post('/my/lists/:id/cards/bulk-status', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const setId = Number(req.params.id);
  const desired = req.body.mode === 'activate' ? 1 : req.body.mode === 'deactivate' ? 0 : null;
  const cardIds = normalizeIds(req.body.card_ids);
  const fallback = `/my/lists/${setId}`;
  const backUrl = req.body.redirectTo || req.get('referer') || fallback;
  if (desired === null || cardIds.length === 0) return res.redirect(backUrl);
  try {
    const set = await get('SELECT * FROM sets WHERE id = ?', [setId]);
    if (!set) return res.redirect('/my/lists');
    const shared = await get('SELECT can_edit FROM set_shares WHERE set_id = ? AND user_id = ?', [set.id, userId]);
    const isOwner = set.user_id === userId;
    const isCollaborator = shared && Number(shared.can_edit) === 1;
    if (!isOwner && !shared) return res.redirect(backUrl);

    const placeholders = cardIds.map(() => '?').join(',');
    const cards = await all(
      `SELECT id, active, favorite, memorized FROM cards WHERE id IN (${placeholders}) AND set_id = ?`,
      [...cardIds, set.id]
    );
    if (!cards || cards.length === 0) return res.redirect(backUrl);
    const allowedIds = cards.map(c => c.id);
    const allowedPlaceholders = allowedIds.map(() => '?').join(',');

    if (isOwner || isCollaborator) {
      await run(`UPDATE cards SET active = ? WHERE id IN (${allowedPlaceholders}) AND set_id = ?`, [
        desired,
        ...allowedIds,
        set.id
      ]);
    } else {
      for (const card of cards) {
        const existingOverride = await get('SELECT * FROM card_overrides WHERE card_id = ? AND user_id = ?', [
          card.id,
          userId
        ]);
        const favorite = existingOverride && existingOverride.favorite !== null ? existingOverride.favorite : card.favorite;
        const memorized =
          existingOverride && existingOverride.memorized !== null ? existingOverride.memorized : card.memorized;
        await run(
          'INSERT OR REPLACE INTO card_overrides (card_id, user_id, active, favorite, memorized) VALUES (?,?,?,?,?)',
          [card.id, userId, desired, favorite, memorized]
        );
      }
    }
  } catch (e) {
    console.error('Bulk card status error:', e);
  }
  res.redirect(backUrl);
});

app.post('/my/cards/batch-toggle-active', requireAuth, express.json(), async (req, res) => {
  const userId = req.session.user.id;
  const { card_ids, set_id } = req.body;

  const cardIds = normalizeIds(card_ids);
  const setId = Number(set_id);

  if (cardIds.length === 0 || !setId) {
    return res.status(400).json({ success: false, error: 'Missing parameters' });
  }

  try {
    const set = await get('SELECT * FROM sets WHERE id = ?', [setId]);
    if (!set) {
      return res.status(404).json({ success: false, error: 'List not found' });
    }

    const shared = await get('SELECT can_edit FROM set_shares WHERE set_id = ? AND user_id = ?', [set.id, userId]);
    const isOwner = set.user_id === userId;
    const isCollaborator = shared && Number(shared.can_edit) === 1;

    if (!isOwner && !shared) {
      return res.status(403).json({ success: false, error: 'Access denied' });
    }

    const placeholders = cardIds.map(() => '?').join(',');
    const cards = await all(
      `SELECT c.id, c.active, c.favorite, c.memorized, co.active as override_active
       FROM cards c
       LEFT JOIN card_overrides co ON co.card_id = c.id AND co.user_id = ?
       WHERE c.id IN (${placeholders}) AND c.set_id = ?`,
      [userId, ...cardIds, setId]
    );

    if (cards.length === 0) {
      return res.status(404).json({ success: false, error: 'No valid cards found in this list' });
    }

    const toggled = [];
    for (const card of cards) {
      if (isOwner || isCollaborator) {
        const newStatus = card.active ? 0 : 1;
        await run('UPDATE cards SET active = ? WHERE id = ?', [newStatus, card.id]);
      } else { // Shared read-only user
        const currentEffectiveStatus = card.override_active === null ? card.active : card.override_active;
        const newStatus = currentEffectiveStatus ? 0 : 1;
        await run(
          'INSERT INTO card_overrides (card_id, user_id, active) VALUES (?, ?, ?) ON CONFLICT(card_id, user_id) DO UPDATE SET active = excluded.active',
          [card.id, userId, newStatus]
        );
      }
      toggled.push(card.id);
    }

    res.json({ success: true, toggled });
  } catch (e) {
    console.error('Batch toggle error:', e);
    res.status(500).json({ success: false, error: 'Server error' });
  }
});

app.delete('/my/lists/:id', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  try {
    const set = await get('SELECT * FROM sets WHERE id = ? AND user_id = ?', [req.params.id, userId]);
    if (!set) return res.redirect('/my/lists');
    await run('DELETE FROM set_shares WHERE set_id = ?', [set.id]);
    await run('DELETE FROM user_set_overrides WHERE set_id = ?', [set.id]);
    await run('DELETE FROM cards WHERE set_id = ?', [set.id]);
    await run('DELETE FROM sets WHERE id = ?', [set.id]);
    await renumberSets(userId);
    res.redirect('/my/lists');
  } catch (e) {
    console.error(e);
    res.redirect('/my/lists');
  }
});

// ---------- Themes utilisateur ----------
async function renderThemeList(req, res) {
  const userId = req.session.user.id;
  const isAdmin = req.session.user && req.session.user.role === 'admin';
  try {
    const allThemes = await all(
      `SELECT t.*,
        COALESCE(uto.active, t.active) AS effective_active,
        uto.active AS user_active,
        (SELECT COUNT(*) FROM theme_levels l WHERE l.theme_id = t.id) AS level_count
       FROM themes t
       LEFT JOIN user_theme_overrides uto ON uto.theme_id = t.id AND uto.user_id = ?
       WHERE t.user_id IS NULL
       ORDER BY t.display_no ASC, t.id ASC`,
      [userId]
    );

    // Build hierarchy
    const themeMap = new Map();
    const rootThemes = [];

    // First pass: create nodes
    allThemes.forEach(t => {
      t.children = [];
      themeMap.set(t.id, t);
    });

    // Second pass: link parents and children
    allThemes.forEach(t => {
      if (t.parent_id && themeMap.has(t.parent_id)) {
        themeMap.get(t.parent_id).children.push(t);
      } else {
        rootThemes.push(t);
      }
    });

    // Filter active/inactive based on root themes, but keep structure
    // For the view, we might want to pass the whole tree and let the view decide visibility
    // But existing logic separates active/inactive.
    // Let's keep existing logic for roots, and children are properties of roots.

    const activeThemes = rootThemes.filter(t => Number(t.effective_active) === 1 && Number(t.active) === 1);
    const inactiveProposed = rootThemes.filter(t => Number(t.active) === 0);
    const inactivePersonal = rootThemes.filter(t => Number(t.active) === 1 && Number(t.effective_active) === 0);

    res.render('themes', {
      activeThemes,
      inactiveProposed,
      inactivePersonal,
      isAdmin,
      pageClass: 'page-compact'
    });
  } catch (e) {
    console.error(e);
    res.render('themes', { activeThemes: [], inactiveProposed: [], inactivePersonal: [], isAdmin, pageClass: 'page-compact' });
  }
}

app.get('/themes', requireAuth, renderThemeList);
app.get('/my/themes', requireAuth, renderThemeList);

app.post('/themes/:id/toggle-visibility', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const backUrl = req.get('referer') || '/themes';
  try {
    const theme = await get(
      `SELECT t.*, COALESCE(uto.active, t.active) AS effective_active
       FROM themes t
       LEFT JOIN user_theme_overrides uto ON uto.theme_id = t.id AND uto.user_id = ?
       WHERE t.id = ?`,
      [userId, req.params.id]
    );
    if (!theme) return res.redirect('/themes');
    if (theme.user_id && theme.user_id !== userId) return res.redirect('/themes');
    if (!theme.active) return res.redirect(backUrl);
    const nextStatus = theme.effective_active ? 0 : 1;

    // Apply the same visibility to the theme and, if it's a parent, all direct subthemes.
    const idsToUpdate = [theme.id];
    if (!theme.parent_id) {
      const children = await all('SELECT id FROM themes WHERE parent_id = ?', [theme.id]);
      children.forEach(c => idsToUpdate.push(c.id));
    }

    for (const id of idsToUpdate) {
      await run(
        'INSERT INTO user_theme_overrides (theme_id, user_id, active) VALUES (?,?,?) ON CONFLICT(theme_id, user_id) DO UPDATE SET active=excluded.active',
        [id, userId, nextStatus]
      );
    }
  } catch (e) {
    console.error('Toggle theme visibility error:', e);
  }
  res.redirect(backUrl);
});

// ---------- Sets (Quizlet-like) ----------
app.get('/my/themes/new', requireAuth, async (req, res) => {
  res.render('my_theme_form', { theme: null, action: '/my/themes' });
});

app.post('/my/themes', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const { name } = req.body;
  if (!name) return res.redirect('/themes');
  try {
    const displayNo = await nextDisplayNo('themes', userId);
    const newId = await findFirstAvailableId('themes');
    await run('INSERT INTO themes (id, name, user_id, created_at, display_no) VALUES (?,?,?, CURRENT_TIMESTAMP, ?)', [newId, name, userId, displayNo]);
    res.redirect('/themes');
  } catch (e) {
    console.error(e);
    res.redirect('/themes');
  }
});

app.get('/my/themes/:id/edit', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  try {
    const theme = await get('SELECT * FROM themes WHERE id = ? AND user_id = ?', [req.params.id, userId]);
    if (!theme) return res.redirect('/themes');
    res.render('my_theme_form', { theme, action: `/my/themes/${theme.id}?_method=PUT` });
  } catch (e) {
    console.error(e);
    res.redirect('/themes');
  }
});

app.put('/my/themes/:id', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const { name } = req.body;
  try {
    await run('UPDATE themes SET name = ? WHERE id = ? AND user_id = ?', [name, req.params.id, userId]);
    res.redirect('/themes');
  } catch (e) {
    console.error(e);
    res.redirect('/themes');
  }
});

app.delete('/my/themes/:id', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  try {
    await run(
      'UPDATE words SET level_id = NULL WHERE level_id IN (SELECT id FROM theme_levels WHERE theme_id = ?) AND user_id = ?',
      [req.params.id, userId]
    );
    await run('UPDATE words SET theme_id = NULL WHERE theme_id = ? AND user_id = ?', [req.params.id, userId]);
    await run('DELETE FROM theme_levels WHERE theme_id = ?', [req.params.id]);
    await run('DELETE FROM user_theme_overrides WHERE theme_id = ?', [req.params.id]);
    await run('DELETE FROM themes WHERE id = ? AND user_id = ?', [req.params.id, userId]);
    await renumberThemes(userId);
    res.redirect('/themes');
  } catch (e) {
    console.error(e);
    res.redirect('/themes');
  }
});

app.get('/themes/:id', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const isAdmin = req.session.user && req.session.user.role === 'admin';
  try {
    const theme = await get(
      `SELECT t.*, COALESCE(uto.active, t.active) AS effective_active, uto.active AS user_active
       FROM themes t
       LEFT JOIN user_theme_overrides uto ON uto.theme_id = t.id AND uto.user_id = ?
       WHERE t.id = ?`,
      [userId, req.params.id]
    );
    if (!theme || (theme.user_id && theme.user_id !== userId) || (!theme.active && !isAdmin)) {
      return res.redirect('/themes');
    }
    const words = await all(
      `SELECT w.id,
        w.hebrew,
        w.transliteration,
        w.french,
        w.active AS global_active,
        w.user_id,
        uwo.active AS override_active,
        CASE WHEN w.active = 0 OR t.active = 0 OR COALESCE(uto.active, 1) = 0 THEN 0 ELSE COALESCE(uwo.active, w.active) END AS effective_active,
        CASE WHEN fav.id IS NULL THEN 0 ELSE 1 END AS favorite
       FROM words w
       LEFT JOIN themes t ON t.id = w.theme_id
       LEFT JOIN user_theme_overrides uto ON uto.theme_id = w.theme_id AND uto.user_id = ?
       LEFT JOIN user_word_overrides uwo ON uwo.word_id = w.id AND uwo.user_id = ?
       LEFT JOIN favorites fav ON fav.word_id = w.id AND fav.user_id = ?
       WHERE w.theme_id = ? AND (w.user_id IS NULL OR w.user_id = ?)
       ORDER BY w.id ASC`,
      [userId, userId, userId, theme.id, userId]
    );
    res.render('theme_show', { theme, words, isOwner: theme.user_id === userId, isGlobal: !theme.user_id });
  } catch (e) {
    console.error(e);
    res.redirect('/themes');
  }
});

// ---------- EntraÃ®nement ----------
app.get('/train/setup', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const rootThemes = await getActiveThemeTreeForUser(userId);

  const levels = await getLevelsForUser(userId);
  const sets = await all(
    `SELECT s.id, s.name
       FROM sets s
       LEFT JOIN user_set_overrides uso ON uso.set_id = s.id AND uso.user_id = ?
       WHERE s.user_id = ? AND s.active = 1 AND COALESCE(uso.active, s.active) = 1
       ORDER BY s.id ASC`,
    [userId, userId]
  );
  const sharedSets = await all(
    `SELECT s.id, s.name, owner.display_name AS owner_name
       FROM set_shares sh
       JOIN sets s ON s.id = sh.set_id
       LEFT JOIN user_set_overrides uso ON uso.set_id = s.id AND uso.user_id = ?
       JOIN users owner ON owner.id = s.user_id
       WHERE sh.user_id = ? AND s.active = 1 AND COALESCE(uso.active, s.active) = 1
       ORDER BY s.id ASC`,
    [userId, userId]
  );
  const state = req.session.trainState || null;
  const useSessionParams = (req.query.current === '1' || req.query.current === 'true' || req.query.source === 'session');

  const storedDefaults = await getUserTrainDefaults(userId);
  const defaultParams = await filterTrainParamsForUser(userId, storedDefaults || DEFAULT_TRAIN_PARAMS);

  const baseParams = {
    ...defaultParams
  };

  let params = { ...baseParams };
  if (useSessionParams && state) {
    const allowedThemeIds = new Set(await getActiveThemeIdsForUser(userId));
    const themeIdsRaw = Array.isArray(state.theme_ids) ? state.theme_ids : state.theme_ids ? [state.theme_ids] : [];
    const themeIds = themeIdsRaw
      .flatMap(t => String(t).split(','))
      .map(id => Number(id))
      .filter(id => !Number.isNaN(id) && allowedThemeIds.has(id));
    const allowedSets = new Set(await getAccessibleSetIds(userId));
    const setIdsRaw = Array.isArray(state.set_ids) ? state.set_ids : state.set_ids ? [state.set_ids] : [];
    const setIds = setIdsRaw
      .flatMap(s => String(s).split(','))
      .map(id => Number(id))
      .filter(id => !Number.isNaN(id) && allowedSets.has(id));
    const remainingVal = state.remaining === null || typeof state.remaining === 'undefined' ? baseParams.remaining : state.remaining;
    const totalVal = state.total === null || typeof state.total === 'undefined' ? remainingVal : state.total;
    params = {
      ...baseParams,
      modes: normalizeModes(state.modes || state.mode),
      rev_mode: state.rev_mode || baseParams.rev_mode,
      show_phonetic: typeof state.show_phonetic === 'undefined' ? baseParams.show_phonetic : state.show_phonetic,
      theme_ids: themeIds,
      set_ids: setIds,
      level_id: state.level_id || '',
      scope: state.scope || baseParams.scope,
      remaining: remainingVal,
      total: totalVal
    };
  }

  res.render('train_setup', {
    themes: rootThemes,
    levels,
    sets,
    sharedSets,
    params
  });
});

app.post('/train/session', requireAuth, async (req, res) => {
  const { modes, mode, theme_ids, set_ids, level_id, rev_mode, remaining, show_phonetic, total } = req.body;
  const rawModes = modes || req.body['modes[]'] || mode;
  const modeList = normalizeModes(rawModes);
  const themeRaw = Array.isArray(theme_ids) ? theme_ids.filter(Boolean) : theme_ids ? [theme_ids] : [];
  const setRaw = Array.isArray(set_ids) ? set_ids.filter(Boolean) : set_ids ? [set_ids] : [];
  const themeIdsRaw = themeRaw.flatMap(t => String(t).split(',')).filter(Boolean).map(id => Number(id)).filter(Boolean);
  const setListRaw = setRaw.flatMap(s => String(s).split(',')).filter(Boolean).map(Number).filter(Boolean);
  const allowedSetIds = await getAccessibleSetIds(req.session.user.id);
  const allowedSetSet = new Set(allowedSetIds);
  const allowedThemeIds = new Set(await getActiveThemeIdsForUser(req.session.user.id));
  const themeList = themeIdsRaw.filter(id => allowedThemeIds.has(id));
  const setList = setListRaw.filter(id => allowedSetSet.has(id));
  const hasThemes = themeList.length > 0;
  const hasSets = setList.length > 0;
  const source = hasThemes && hasSets ? 'mixed' : hasSets ? 'cards' : 'words';
  let remain = remaining === 'all' ? null : Number(remaining || 10);
  const showPhonetic = String(show_phonetic) === '0' ? 0 : 1;
  const conflictChoice = req.body.conflict_choice;
  const requestedRemaining = remaining === 'all' ? 'all' : remain || DEFAULT_TRAIN_PARAMS.remaining;
  const requestedTotal = total === 'all' ? 'all' : requestedRemaining;

  if (themeList.length === 0 && setList.length === 0) {
    const userId = req.session.user.id;
    const themes = await getActiveThemeTreeForUser(userId);
    const levels = await getLevelsForUser(userId);
    const sets = await all(
      `SELECT s.id, s.name
       FROM sets s
       LEFT JOIN user_set_overrides uso ON uso.set_id = s.id AND uso.user_id = ?
       WHERE s.user_id = ? AND s.active = 1 AND COALESCE(uso.active, s.active) = 1
       ORDER BY s.id ASC`,
      [userId, userId]
    );
    const sharedSets = await all(
      `SELECT s.id, s.name, owner.display_name AS owner_name
       FROM set_shares sh
       JOIN sets s ON s.id = sh.set_id
       LEFT JOIN user_set_overrides uso ON uso.set_id = s.id AND uso.user_id = ?
       JOIN users owner ON owner.id = s.user_id
       WHERE sh.user_id = ? AND s.active = 1 AND COALESCE(uso.active, s.active) = 1
       ORDER BY s.id ASC`,
      [userId, userId]
    );
    return res.render('train_setup', {
      themes,
      levels,
      sets,
      sharedSets,
      error: 'Selectionne au moins un theme ou une liste.',
      params: {
        modes: modeList,
        rev_mode: rev_mode || 'order',
        theme_id: '',
        level_id: level_id || '',
        scope: 'all',
        remaining: requestedRemaining,
        total: requestedTotal,
        show_phonetic: showPhonetic
      }
    });
  }

  const existingState = req.session.trainState;
  if (
    existingState &&
    Number(existingState.answered || 0) > 0 &&
    Number(existingState.remaining || 0) > 0
  ) {
    const existingThemes = normalizeIds(existingState.theme_ids || []);
    const existingSets = normalizeIds(existingState.set_ids || []);
    const overlapThemes = themeList.filter(id => existingThemes.includes(Number(id)));
    const overlapSets = setList.filter(id => existingSets.includes(Number(id)));
    if (overlapThemes.length > 0 || overlapSets.length > 0) {
      if (conflictChoice === 'resume') {
        return res.redirect('/train/resume?choice=resume');
      }
      if (conflictChoice !== 'restart') {
        const overlapThemeNames =
          overlapThemes.length > 0
            ? await all(
              `SELECT id, name FROM themes WHERE id IN (${overlapThemes.map(() => '?').join(',')})`,
              overlapThemes
            )
            : [];
        const overlapSetNames =
          overlapSets.length > 0
            ? await all(
              `SELECT id, name FROM sets WHERE id IN (${overlapSets.map(() => '?').join(',')})`,
              overlapSets
            )
            : [];
        return res.render('train_conflict', {
          overlapThemes: overlapThemeNames,
          overlapSets: overlapSetNames,
          params: {
            modes: modeList,
            rev_mode: rev_mode || 'order',
            show_phonetic: showPhonetic,
            theme_ids: themeList,
            set_ids: setList,
            level_id: level_id || '',
            remaining: requestedRemaining,
            total: requestedTotal
          }
        });
      }
    }
  }

  let poolWords = null;
  let poolCards = null;
  const themeOrderMap = new Map(themeList.map((id, idx) => [Number(id), idx]));
  const setOrderMap = new Map(setList.map((id, idx) => [Number(id), idx]));

  // Preload pools when we need ordering
  const needOrderedPools = (rev_mode || '').toLowerCase() === 'order';
  if (needOrderedPools) {
    if (hasSets) {
      const cards = await getEffectiveCardsForUser(req.session.user.id, setList);
      poolCards = cards.map(c => ({
        ...c,
        order_set: setOrderMap.has(Number(c.set_id)) ? setOrderMap.get(Number(c.set_id)) : Number.MAX_SAFE_INTEGER
      })).sort((a, b) => {
        if (a.order_set !== b.order_set) return a.order_set - b.order_set;
        const pa = Number(a.position);
        const pb = Number(b.position);
        if (!Number.isNaN(pa) && !Number.isNaN(pb) && pa !== pb) return pa - pb;
        return Number(a.id) - Number(b.id);
      });
    }
    if (hasThemes) {
      const words = await getWordPoolForUser(req.session.user.id, {
        theme_ids: themeList,
        level_id: level_id || null,
        rev_mode: rev_mode || 'order',
        scope: 'all'
      });
      poolWords = words
        .map(w => ({
          ...w,
          order_theme: themeOrderMap.has(Number(w.theme_id)) ? themeOrderMap.get(Number(w.theme_id)) : Number.MAX_SAFE_INTEGER
        }))
        .sort((a, b) => {
          if (a.order_theme !== b.order_theme) return a.order_theme - b.order_theme;
          const pa = Number(a.position);
          const pb = Number(b.position);
          if (!Number.isNaN(pa) && !Number.isNaN(pb) && pa !== pb) return pa - pb;
          return Number(a.id) - Number(b.id);
        });
    }
  }

  if (remain === null) {
    const baseFilters = {
      theme_ids: themeList,
      level_id: level_id || null,
      rev_mode: rev_mode || 'order',
      scope: 'all'
    };
    if (!poolCards && hasSets) {
      poolCards = await getEffectiveCardsForUser(req.session.user.id, setList);
    }
    if (!poolWords && hasThemes) {
      poolWords = await getWordPoolForUser(req.session.user.id, baseFilters);
    }
    if (source === 'cards') {
      remain = poolCards ? poolCards.length : 0;
    } else if (source === 'words') {
      remain = poolWords ? poolWords.length : 0;
    } else {
      const cardCount = poolCards ? poolCards.length : 0;
      const wordCount = poolWords ? poolWords.length : 0;
      remain = cardCount + wordCount;
    }
  }

  if (!remain || Number(remain) <= 0) {
    const userId = req.session.user.id;
    const themes = await getActiveThemeTreeForUser(userId);
    const levels = await getLevelsForUser(userId);
    const sets = await all(
      `SELECT s.id, s.name
       FROM sets s
       LEFT JOIN user_set_overrides uso ON uso.set_id = s.id AND uso.user_id = ?
       WHERE s.user_id = ? AND s.active = 1 AND COALESCE(uso.active, s.active) = 1
       ORDER BY s.created_at DESC`,
      [userId, userId]
    );
    const sharedSets = await all(
      `SELECT s.id, s.name, owner.display_name AS owner_name
       FROM set_shares sh
       JOIN sets s ON s.id = sh.set_id
       LEFT JOIN user_set_overrides uso ON uso.set_id = s.id AND uso.user_id = ?
       JOIN users owner ON owner.id = s.user_id
       WHERE sh.user_id = ? AND s.active = 1 AND COALESCE(uso.active, s.active) = 1
       ORDER BY s.created_at DESC`,
      [userId, userId]
    );
    return res.render('train_setup', {
      themes,
      levels,
      sets,
      sharedSets,
      error: 'Aucun mot disponible avec cette selection.',
      params: {
        modes: modeList,
        rev_mode: rev_mode || 'order',
        theme_id: '',
        level_id: level_id || '',
        scope: 'all',
        remaining: requestedRemaining,
        total: requestedTotal,
        show_phonetic: showPhonetic
      }
    });
  }

  req.session.trainState = {
    modes: modeList,
    theme_ids: themeList,
    set_ids: setList,
    level_id: level_id || '',
    rev_mode: rev_mode || 'order',
    source,
    scope: 'all',
    remaining: remain,
    total: remain,
    show_phonetic: showPhonetic,
    answered: 0,
    correct: 0,
    usedWordIds: [],
    usedCardIds: [],
    poolWords: poolWords ? poolWords.map(w => w.id) : null,
    poolCards: poolCards ? poolCards.map(c => c.id) : null,
    wordQueue: poolWords ? poolWords.map(w => w.id) : null,
    cardQueue: poolCards ? poolCards.map(c => c.id) : null,
    wordQueueIndex: 0,
    cardQueueIndex: 0
  };
  res.redirect('/train');
});

app.get('/train', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const state = req.session.trainState;
  if (!state) return res.redirect('/train/setup');
  const modeList = normalizeModes(state.modes || state.mode);
  state.modes = modeList;
  state.usedWordIds = Array.isArray(state.usedWordIds)
    ? state.usedWordIds.map(id => Number(id)).filter(id => !Number.isNaN(id))
    : [];
  state.usedCardIds = Array.isArray(state.usedCardIds)
    ? state.usedCardIds.map(id => Number(id)).filter(id => !Number.isNaN(id))
    : [];
  state.wordQueue = Array.isArray(state.wordQueue)
    ? state.wordQueue.map(id => Number(id)).filter(id => !Number.isNaN(id))
    : null;
  state.cardQueue = Array.isArray(state.cardQueue)
    ? state.cardQueue.map(id => Number(id)).filter(id => !Number.isNaN(id))
    : null;
  state.wordQueueIndex = Number.isInteger(state.wordQueueIndex) ? state.wordQueueIndex : 0;
  state.cardQueueIndex = Number.isInteger(state.cardQueueIndex) ? state.cardQueueIndex : 0;
  const numericRemaining = Number(state.remaining);
  const numericTotal = Number(state.total);
  state.remaining = Number.isFinite(numericRemaining) ? numericRemaining : (Number.isFinite(numericTotal) ? numericTotal : 0);
  state.total = Number.isFinite(numericTotal) ? numericTotal : state.remaining;
  const allowedSets = await getAccessibleSetIds(userId);
  state.set_ids = Array.isArray(state.set_ids) ? state.set_ids.filter(id => allowedSets.includes(Number(id))) : [];
  const replay = state.replay || null;
  const mode = modeList[0] || MODE_FLASHCARDS;
  const questionMode = replay && replay.questionMode ? replay.questionMode : pickQuestionMode(modeList);
  state.currentMode = questionMode;

  const remainingCount = Number(state.remaining || 0);
  const totalCount = Number(state.total || 0);
  const answeredCount = Number(state.answered || 0);
  const correctCount = Number(state.correct || 0);
  let currentItemId = '';
  let currentFavorite = 0;
  const renderTrain = (payload) =>
    res.render('train', {
      currentItemId,
      currentFavorite,
      ...payload
    });
  const allowedThemeIds = new Set(await getActiveThemeIdsForUser(userId));
  const themeListRaw = state.theme_ids || [];
  const themeList = themeListRaw.filter(id => allowedThemeIds.has(Number(id)));
  state.theme_ids = themeList;
  const selectedSetIds = state.set_ids || [];
  const hasThemes = themeList.length > 0;
  const hasSets = selectedSetIds.length > 0;
  let source = state.source || (hasSets ? 'cards' : 'words');
  const setOrder = new Map(selectedSetIds.map((id, idx) => [Number(id), idx]));
  if (source === 'mixed' && !hasSets) {
    source = hasThemes ? 'words' : 'cards';
  } else if (source === 'mixed' && !hasThemes) {
    source = 'cards';
  }
  state.source = source;

  const filters = {
    theme_id: themeList.length === 1 ? themeList[0] : null,
    theme_ids: themeList,
    level_id: state.level_id || null,
    show_phonetic: typeof state.show_phonetic === 'undefined' ? 1 : state.show_phonetic,
    rev_mode: state.rev_mode || 'random',
    scope: 'all',
    scope_list: [],
    source,
    set_ids: selectedSetIds,
    theme_order: themeList,
    set_order: selectedSetIds
  };

  const themes = await getActiveThemesForUser(userId);

  if (remainingCount <= 0) {
    delete req.session.trainState;
  } else {
    req.session.trainState = state;
  }

  if (remainingCount <= 0) {
    return renderTrain({
      word: null,
      message: 'Session terminee.',
      result: null,
      mode,
      modes: modeList,
      questionMode,
      filters,
      options: null,
      themes,
      remaining: 0,
      total: totalCount,
      answered: answeredCount,
      correct: correctCount,
      nextUrl: null
    });
  }

  try {
    const reviewMode = (state.rev_mode || 'order').toLowerCase();
    if (filters.source === 'mixed') {
      const allowedSetIds = await getAccessibleSetIds(userId);
      const filteredIds = (filters.set_ids || []).filter(id => allowedSetIds.includes(Number(id)));
      const cardsPool = filteredIds.length > 0 ? await getEffectiveCardsForUser(userId, filteredIds) : [];
      const usedCardSet = new Set(state.usedCardIds.map(id => Number(id)).filter(id => !Number.isNaN(id)));
      let availableCards = cardsPool.filter(c => !usedCardSet.has(Number(c.id)));
      if (availableCards.length === 0 && cardsPool.length > 0 && reviewMode !== 'order') {
        state.usedCardIds = [];
        availableCards = cardsPool;
      }

      let chosenCard = null;
      if (replay && replay.type === 'card') {
        const cardId = Number(replay.id);
        const card = await get(
          `SELECT c.*, COALESCE(co.favorite, c.favorite) AS effective_favorite
           FROM cards c
           LEFT JOIN card_overrides co ON co.card_id = c.id AND co.user_id = ?
           WHERE c.id = ?`,
          [userId, cardId]
        );
        const stillAllowed = card && filteredIds.includes(Number(card.set_id));
        if (card && stillAllowed) {
          chosenCard = { ...card, favorite: card.effective_favorite };
        }
        state.replay = null;
      }

      let word = null;
      if (replay && replay.type === 'word') {
        const candidate = await get('SELECT * FROM words WHERE id = ?', [replay.id]);
        if (candidate) {
          word = candidate;
        } else {
          state.replay = null;
        }
      }

      const excludedIds = state.usedWordIds.map(id => Number(id)).filter(id => !Number.isNaN(id));
      const wordQueue = Array.isArray(state.wordQueue) ? state.wordQueue : null;
      if (!word && (reviewMode !== 'order' || availableCards.length === 0)) {
        if (reviewMode === 'order' && wordQueue && wordQueue.length > 0) {
          if (state.wordQueueIndex < wordQueue.length) {
            const nextId = wordQueue[state.wordQueueIndex];
            const candidate = await get('SELECT * FROM words WHERE id = ?', [nextId]);
            if (candidate) {
              word = candidate;
              state.wordQueueIndex += 1;
              if (!state.usedWordIds.includes(Number(nextId))) {
                state.usedWordIds.push(Number(nextId));
              }
            }
          }
        } else {
          const fetcher = fetchNextWord;
          word = await fetcher(userId, { ...filters, source: 'words', excludedIds });

          if (!word && excludedIds.length > 0) {
            state.usedWordIds = [];
            word = await fetcher(userId, { ...filters, source: 'words', excludedIds: [] });
          }
          if (word) {
            const wordId = Number(word.id);
            if (!state.usedWordIds.includes(wordId)) {
              state.usedWordIds.push(wordId);
            }
          }
        }
      }

      const renderCardChoice = async (card) => {
        const cardId = Number(card.id);
        const cardKey = `card_${cardId}`;
        const wordLike = { id: cardKey, hebrew: card.hebrew, transliteration: card.transliteration, french: card.french, favorite: card.favorite };
        currentItemId = wordLike.id;
        currentFavorite = card.favorite ? 1 : 0;
        if (!state.usedCardIds.includes(cardId)) {
          state.usedCardIds.push(cardId);
        }
        state.pendingQuestion = { type: 'card', id: cardId, questionMode };
        req.session.trainState = state;

        if (questionMode === MODE_WRITTEN) {
          return renderTrain({
            word: wordLike,
            message: null,
            result: null,
            mode,
            modes: modeList,
            questionMode,
            filters,
            options: null,
            themes,
            remaining: remainingCount,
            total: totalCount,
            answered: answeredCount,
            correct: correctCount,
            nextUrl: `/train`
          });
        }

        const poolCopy = cardsPool.filter(c => c.id !== cardId);
        const distractors = [];
        while (poolCopy.length > 0 && distractors.length < 3) {
          const idx = Math.floor(Math.random() * poolCopy.length);
          distractors.push(poolCopy.splice(idx, 1)[0]);
        }
        const isReverse = questionMode === MODE_FLASHCARDS_REVERSE;
        const optionsRaw = [
          ...distractors.map(c => ({ id: `card_${c.id}`, label: isReverse ? c.hebrew : c.french })),
          { id: cardKey, label: isReverse ? card.hebrew : card.french }
        ];
        for (let i = optionsRaw.length - 1; i > 0; i--) {
          const j = Math.floor(Math.random() * (i + 1));
          [optionsRaw[i], optionsRaw[j]] = [optionsRaw[j], optionsRaw[i]];
        }
        return renderTrain({
          word: wordLike,
          message: null,
          result: null,
          mode,
          modes: modeList,
          questionMode,
          filters,
          options: optionsRaw,
          themes,
          remaining: remainingCount,
          total: totalCount,
          answered: answeredCount,
          correct: correctCount,
          nextUrl: `/train`
        });
      };

      const renderWordChoice = async (wordChoice) => {
        let fav = wordChoice.fav_id ? 1 : 0;
        if (!fav) {
          const favRow = await get('SELECT id FROM favorites WHERE user_id = ? AND word_id = ?', [userId, wordChoice.id]);
          fav = favRow ? 1 : 0;
        }
        currentItemId = wordChoice.id;
        currentFavorite = fav;
        const isReverse = questionMode === MODE_FLASHCARDS_REVERSE;
        const options =
          wordChoice && (questionMode === MODE_FLASHCARDS || questionMode === MODE_FLASHCARDS_REVERSE)
            ? await getFlashcardOptions(userId, wordChoice, filters, isReverse)
            : null;
        state.replay = null;
        req.session.trainState = state;
        return renderTrain({
          word: wordChoice,
          message: null,
          result: null,
          mode,
          modes: modeList,
          questionMode,
          filters,
          options,
          themes,
          remaining: remainingCount,
          total: totalCount,
          answered: answeredCount,
          correct: correctCount,
          nextUrl: `/train`
        });
      };

      let choice = null;
      if (chosenCard) {
        choice = { type: 'card', card: chosenCard };
      } else if (word && replay && replay.type === 'word') {
        choice = { type: 'word', word };
      } else if (reviewMode === 'order') {
        if (availableCards.length > 0) {
          availableCards.sort((a, b) => {
            const sa = Number(a.set_id) || 0;
            const sb = Number(b.set_id) || 0;
            const oa = setOrder.has(sa) ? setOrder.get(sa) : 2147483647;
            const ob = setOrder.has(sb) ? setOrder.get(sb) : 2147483647;
            if (oa !== ob) return oa - ob;
            const pa = Number(a.position);
            const pb = Number(b.position);
            if (!Number.isNaN(pa) && !Number.isNaN(pb) && pa !== pb) return pa - pb;
            return Number(a.id) - Number(b.id);
          });
          choice = { type: 'card', card: availableCards[0] };
        } else if (word) {
          choice = { type: 'word', word };
        }
      } else {
        const candidates = [];
        if (availableCards.length > 0) {
          const pickCard = availableCards[Math.floor(Math.random() * availableCards.length)];
          candidates.push({ type: 'card', card: pickCard });
        }
        if (word) {
          candidates.push({ type: 'word', word });
        }
        if (candidates.length > 0) {
          choice = candidates[Math.floor(Math.random() * candidates.length)];
        }
      }

      if (!choice) {
        delete req.session.trainState;
        return renderTrain({
          word: null,
          message: 'Aucun mot pour ces criteres.',
          result: null,
          mode,
          modes: modeList,
          questionMode,
          filters,
          options: null,
          themes,
          remaining: 0,
          total: totalCount,
          answered: answeredCount,
          correct: correctCount,
          nextUrl: null
        });
      }

      if (choice.type === 'card') {
        return renderCardChoice(choice.card);
      }
      return renderWordChoice(choice.word);
    }

    if (filters.source === 'cards') {
      if (replay && replay.type === 'card') {
        const cardId = Number(replay.id);
        const allowedSetIds = await getAccessibleSetIds(userId);
        const card = await get(
          `SELECT c.*, COALESCE(co.favorite, c.favorite) AS effective_favorite
           FROM cards c
           LEFT JOIN card_overrides co ON co.card_id = c.id AND co.user_id = ?
           WHERE c.id = ?`,
          [userId, cardId]
        );
        const stillAllowed = card && allowedSetIds.includes(Number(card.set_id));
        if (card && stillAllowed) {
          const word = { id: `card_${cardId}`, hebrew: card.hebrew, transliteration: card.transliteration, french: card.french, favorite: card.effective_favorite };
          currentItemId = word.id;
          currentFavorite = word.favorite ? 1 : 0;
          const isReverse = questionMode === MODE_FLASHCARDS_REVERSE;
          const cardsPool = await getEffectiveCardsForUser(userId, state.set_ids || []);
          const poolCopy = cardsPool.filter(c => c.id !== cardId);
          const distractors = [];
          while (poolCopy.length > 0 && distractors.length < 3) {
            const idx = Math.floor(Math.random() * poolCopy.length);
            distractors.push(poolCopy.splice(idx, 1)[0]);
          }
          const optionsRaw = [
            ...distractors.map(c => ({ id: `card_${c.id}`, label: isReverse ? c.hebrew : c.french })),
            { id: `card_${cardId}`, label: isReverse ? card.hebrew : card.french }
          ];
          for (let i = optionsRaw.length - 1; i > 0; i--) {
            const j = Math.floor(Math.random() * (i + 1));
            [optionsRaw[i], optionsRaw[j]] = [optionsRaw[j], optionsRaw[i]];
          }
          state.replay = null;
          req.session.trainState = state;
          return renderTrain({
            word,
            message: null,
            result: null,
            mode,
            modes: modeList,
            questionMode,
            filters,
            options: optionsRaw,
            themes,
            remaining: remainingCount,
            total: totalCount,
            answered: answeredCount,
            correct: correctCount,
            nextUrl: `/train`
          });
        } else {
          state.replay = null;
        }
      }

      const allowedSetIds = await getAccessibleSetIds(userId);
      const filteredIds = (filters.set_ids || []).filter(id => allowedSetIds.includes(Number(id)));
      if (filteredIds.length === 0) {
        delete req.session.trainState;
        return renderTrain({
          word: null,
          message: 'Aucune carte disponible.',
          result: null,
          mode,
          modes: modeList,
          questionMode,
          filters: { ...filters, set_ids: [] },
          options: null,
          themes,
          remaining: 0,
          total: totalCount,
          answered: answeredCount,
          correct: correctCount,
          nextUrl: null
        });
      }
      const cardsPool = await getEffectiveCardsForUser(userId, filteredIds);
      if (!cardsPool || cardsPool.length === 0) {
        req.session.trainState = state;
        return renderTrain({
          word: null,
          message: 'Aucune carte dans ces listes.',
          result: null,
          mode,
          modes: modeList,
          questionMode,
          filters,
          options: null,
          themes,
          remaining: remainingCount,
          total: totalCount,
          answered: answeredCount,
          correct: correctCount,
          nextUrl: null
        });
      }

      let pick = null;
      const cardQueue = Array.isArray(state.cardQueue) ? state.cardQueue : [];

      if (reviewMode === 'order') {
        // Advance strictly following queue order; once queue is exhausted, stop picking cards
        if (state.cardQueueIndex < cardQueue.length) {
          const nextId = cardQueue[state.cardQueueIndex];
          pick = cardsPool.find(c => Number(c.id) === Number(nextId));
          state.cardQueueIndex += 1;
        } else {
          pick = null;
        }
      } else {
        let availableCards = cardsPool;
        if (state.usedCardIds.length > 0) {
          const usedSet = new Set(state.usedCardIds.map(id => Number(id)).filter(id => !Number.isNaN(id)));
          availableCards = cardsPool.filter(c => !usedSet.has(Number(c.id)));
        }
        if (availableCards.length === 0 && cardsPool.length > 0) {
          state.usedCardIds = [];
          availableCards = cardsPool;
        }
        pick = availableCards[Math.floor(Math.random() * availableCards.length)];
      }
      if (pick) {
        const cardId = Number(pick.id);
        const cardKey = `card_${cardId}`;
        const word = { id: cardKey, hebrew: pick.hebrew, transliteration: pick.transliteration, french: pick.french, favorite: pick.favorite };
        currentItemId = word.id;
        currentFavorite = word.favorite ? 1 : 0;
        if (!state.usedCardIds.includes(cardId)) {
          state.usedCardIds.push(cardId);
        }
        state.pendingQuestion = { type: 'card', id: cardId, questionMode };
        req.session.trainState = state;

        if (questionMode === MODE_WRITTEN) {
          return renderTrain({
            word,
            message: null,
            result: null,
            mode,
            modes: modeList,
            questionMode,
            filters,
            options: null,
            themes,
            remaining: remainingCount,
            total: totalCount,
            answered: answeredCount,
            correct: correctCount,
            nextUrl: `/train`
          });
        }
        const poolCopy = cardsPool.filter(c => c.id !== pick.id);
        const distractors = [];
        while (poolCopy.length > 0 && distractors.length < 3) {
          const idx = Math.floor(Math.random() * poolCopy.length);
          distractors.push(poolCopy.splice(idx, 1)[0]);
        }
        const isReverse = questionMode === MODE_FLASHCARDS_REVERSE;
        const optionsRaw = [
          ...distractors.map(c => ({ id: `card_${c.id}`, label: isReverse ? c.hebrew : c.french })),
          { id: cardKey, label: isReverse ? pick.hebrew : pick.french }
        ];
        for (let i = optionsRaw.length - 1; i > 0; i--) {
          const j = Math.floor(Math.random() * (i + 1));
          [optionsRaw[i], optionsRaw[j]] = [optionsRaw[j], optionsRaw[i]];
        }
        return renderTrain({
          word,
          message: null,
          result: null,
          mode,
          modes: modeList,
          questionMode,
          filters,
          options: optionsRaw,
          themes,
          remaining: remainingCount,
          total: totalCount,
          answered: answeredCount,
          correct: correctCount,
          nextUrl: `/train`
        });
      }
    }

    let word = null;
    if (replay && replay.type === 'word') {
      const candidate = await get('SELECT * FROM words WHERE id = ?', [replay.id]);
      if (candidate) {
        word = candidate;
      } else {
        state.replay = null;
      }
    }
    if (!word) {
      const excludedIds = state.usedWordIds.map(id => Number(id)).filter(id => !Number.isNaN(id));
      const fetcher = reviewMode === 'order' ? fetchNextOrderedWord : fetchNextWord;
      word = await fetcher(userId, { ...filters, excludedIds });

      // If no word found and we have excluded words, it means we exhausted the pool.
      // Reset used words and try again to allow repetition.
      if (!word && excludedIds.length > 0) {
        state.usedWordIds = [];
        // We pass empty excludedIds to fetch from the full pool again
        word = await fetcher(userId, { ...filters, excludedIds: [] });
      }
      if (word) {
        const wordId = Number(word.id);
        if (!state.usedWordIds.includes(wordId)) {
          state.usedWordIds.push(wordId);
        }
      }
      req.session.trainState = state;
    }
    if (word) {
      let fav = word.fav_id ? 1 : 0;
      if (!fav) {
        const favRow = await get('SELECT id FROM favorites WHERE user_id = ? AND word_id = ?', [userId, word.id]);
        fav = favRow ? 1 : 0;
      }
      currentItemId = word.id;
      currentFavorite = fav;
    }

    const isReverse = questionMode === MODE_FLASHCARDS_REVERSE;
    const options =
      word && (questionMode === MODE_FLASHCARDS || questionMode === MODE_FLASHCARDS_REVERSE)
        ? await getFlashcardOptions(userId, word, filters, isReverse)
        : null;
    const message = !word ? 'Aucun mot pour ces criteres.' : null;
    state.replay = null;
    req.session.trainState = state;
    renderTrain({
      word,
      message,
      result: null,
      mode,
      modes: modeList,
      questionMode,
      filters,
      options,
      themes,
      remaining: remainingCount,
      total: totalCount,
      answered: answeredCount,
      correct: correctCount,
      nextUrl: `/train`
    });
  } catch (e) {
    console.error(e);
    renderTrain({
      word: null,
      message: 'Erreur serveur.',
      result: null,
      mode,
      modes: modeList,
      questionMode,
      filters,
      options: null,
      themes: [],
      remaining: remainingCount,
      total: totalCount,
      answered: answeredCount,
      correct: correctCount,
      nextUrl: null
    });
  }
});

app.post('/train/answer', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const { word_id, user_answer, question_mode } = req.body;
  const state = req.session.trainState;
  if (!state) return res.redirect('/train/setup');

  const modeList = normalizeModes(state.modes || state.mode);
  state.modes = modeList;
  const activeMode = normalizeMode(question_mode || state.currentMode || modeList[0]);
  const selectedSetIds = state.set_ids || [];
  const source = state.source || (selectedSetIds.length > 0 ? 'cards' : 'words');
  const filters = {
    theme_id: state.theme_id || null,
    theme_ids: state.theme_ids || [],
    level_id: state.level_id || null,
    show_phonetic: typeof state.show_phonetic === 'undefined' ? 1 : state.show_phonetic,
    rev_mode: state.rev_mode || 'random',
    scope: state.scope || 'all',
    scope_list: [],
    source,
    set_ids: selectedSetIds
  };

  const prevTotal = Number(state.total);
  const totalCount = Number.isFinite(prevTotal) && prevTotal > 0 ? prevTotal : (Number.isFinite(Number(state.remaining)) ? Number(state.remaining) : 1);
  const prevRemaining = Number(state.remaining);
  const remainingBefore = Number.isFinite(prevRemaining) ? prevRemaining : totalCount;
  const remainingCount = Math.max(0, remainingBefore - 1);
  const prevAnswered = Number(state.answered);
  const answeredCount = Number.isFinite(prevAnswered)
    ? prevAnswered + 1
    : Math.max(0, totalCount - remainingBefore) + 1;
  let correctCount = Number(state.correct || 0);
  const normalizedAnswer = (user_answer || '').trim().toLowerCase();
  let currentItemId = '';
  let currentFavorite = 0;
  const renderTrainAnswer = (payload) =>
    res.render('train', {
      currentItemId,
      currentFavorite,
      ...payload
    });

  try {
    const themes = await getActiveThemesForUser(userId);

    if ((filters.source === 'cards' || filters.source === 'mixed') && String(word_id).startsWith('card_')) {
      const cardId = Number(String(word_id).replace('card_', ''));
      const card = await get(
        `SELECT c.*, COALESCE(co.favorite, c.favorite) AS effective_favorite,
                co.active AS override_active,
                co.memorized AS override_memorized
         FROM cards c
         LEFT JOIN card_overrides co ON co.card_id = c.id AND co.user_id = ?
         WHERE c.id = ?`,
        [userId, cardId]
      );
      if (!card) return res.redirect('/train');
      currentItemId = `card_${cardId}`;
      currentFavorite = card.effective_favorite ? 1 : 0;
      const isCorrect = normalizedAnswer && normalizedAnswer === (card.french || '').trim().toLowerCase();
      if (isCorrect) correctCount += 1;
      await upsertCardProgress(userId, cardId, isCorrect);
      if (remainingCount > 0) {
        req.session.trainState = { ...state, correct: correctCount, answered: answeredCount, remaining: remainingCount, total: totalCount, modes: modeList };
      } else {
        delete req.session.trainState;
      }

      const result = {
        isCorrect,
        hebrew: card.hebrew,
        transliteration: card.transliteration,
        french: card.french
      };

      const nextUrl = remainingCount > 0 ? `/train` : null;

      return renderTrainAnswer({
        word: null,
        message: remainingCount > 0 ? null : 'Session terminee.',
        result,
        mode: modeList[0] || activeMode,
        modes: modeList,
        questionMode: MODE_WRITTEN,
        filters,
        options: null,
        themes,
        remaining: remainingCount,
        total: totalCount,
        answered: answeredCount,
        correct: correctCount,
        nextUrl
      });
    }

    const word = await get('SELECT * FROM words WHERE id = ?', [word_id]);
    if (!word) return res.redirect('/train');
    currentItemId = String(word_id);
    const favRow = await get('SELECT id FROM favorites WHERE user_id = ? AND word_id = ?', [userId, word_id]);
    currentFavorite = favRow ? 1 : 0;

    const isCorrect = normalizedAnswer && normalizedAnswer === (word.french || '').trim().toLowerCase();
    if (isCorrect) correctCount += 1;

    const progressUpdate = await upsertProgress(userId, word_id, isCorrect);
    if (remainingCount > 0) {
      req.session.trainState = { ...state, correct: correctCount, answered: answeredCount, remaining: remainingCount, total: totalCount, modes: modeList, replay: null };
    } else {
      delete req.session.trainState;
    }

    const result = {
      isCorrect,
      hebrew: word.hebrew,
      transliteration: word.transliteration,
      french: word.french,
      newStrength: progressUpdate ? progressUpdate.strength : null
    };

    const nextUrl = remainingCount > 0 ? `/train` : null;

    return renderTrainAnswer({
      word: null,
      message: remainingCount > 0 ? null : 'Session terminee.',
      result,
      mode: modeList[0] || activeMode,
      modes: modeList,
      questionMode: MODE_WRITTEN,
      filters,
      options: null,
      themes,
      remaining: remainingCount,
      total: totalCount,
      answered: answeredCount,
      correct: correctCount,
      nextUrl
    });
  } catch (e) {
    console.error(e);
    return res.redirect('/train');
  }
});

app.post('/train/favorite', requireAuth, express.json(), async (req, res) => {
  const userId = req.session.user.id;
  const rawId = req.body.item_id;
  if (!rawId) return res.status(400).json({ success: false, error: 'Missing item_id' });

  try {
    const strId = String(rawId);
    if (strId.startsWith('card_')) {
      const cardId = Number(strId.replace('card_', ''));
      if (!cardId) return res.status(400).json({ success: false, error: 'Invalid card id' });
      const card = await get(
        `SELECT c.id, c.set_id, c.favorite,
                co.favorite AS override_favorite,
                co.active AS override_active,
                co.memorized AS override_memorized
         FROM cards c
         LEFT JOIN card_overrides co ON co.card_id = c.id AND co.user_id = ?
         WHERE c.id = ?`,
        [userId, cardId]
      );
      if (!card) return res.status(404).json({ success: false, error: 'Not found' });
      const accessibleSets = await getAccessibleSetIds(userId);
      if (!accessibleSets.includes(Number(card.set_id))) {
        return res.status(403).json({ success: false, error: 'Forbidden' });
      }
      const currentFav =
        card.override_favorite === null || typeof card.override_favorite === 'undefined'
          ? card.favorite
          : card.override_favorite;
      const nextFav = currentFav ? 0 : 1;
      const existingOverride = await get(
        'SELECT active, memorized FROM card_overrides WHERE card_id = ? AND user_id = ?',
        [cardId, userId]
      );
      const activeVal =
        existingOverride && typeof existingOverride.active !== 'undefined'
          ? existingOverride.active
          : card.override_active;
      const memoVal =
        existingOverride && typeof existingOverride.memorized !== 'undefined'
          ? existingOverride.memorized
          : card.override_memorized;
      await run(
        'INSERT OR REPLACE INTO card_overrides (card_id, user_id, active, favorite, memorized) VALUES (?,?,?,?,?)',
        [cardId, userId, activeVal, nextFav, memoVal]
      );
      return res.json({ success: true, favorite: nextFav });
    }

    const wordId = Number(strId);
    if (!wordId) return res.status(400).json({ success: false, error: 'Invalid word id' });
    const word = await get('SELECT id FROM words WHERE id = ?', [wordId]);
    if (!word) return res.status(404).json({ success: false, error: 'Not found' });
    const existing = await get('SELECT id FROM favorites WHERE user_id = ? AND word_id = ?', [userId, wordId]);
    if (existing) {
      await run('DELETE FROM favorites WHERE id = ?', [existing.id]);
      return res.json({ success: true, favorite: 0 });
    }
    await run('INSERT INTO favorites (user_id, word_id) VALUES (?,?)', [userId, wordId]);
    return res.json({ success: true, favorite: 1 });
  } catch (e) {
    console.error('Favorite toggle error:', e);
    return res.status(500).json({ success: false, error: 'Server error' });
  }
});
app.post('/favorites/:id', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const redirectTo = req.body.redirectTo || '/train';
  try {
    const existing = await get('SELECT * FROM favorites WHERE user_id = ? AND word_id = ?', [userId, req.params.id]);
    if (existing) {
      await run('DELETE FROM favorites WHERE id = ?', [existing.id]);
    } else {
      await run('INSERT INTO favorites (user_id, word_id) VALUES (?,?)', [userId, req.params.id]);
    }
    res.redirect(redirectTo);
  } catch (e) {
    console.error(e);
    res.redirect(redirectTo);
  }
});

// Reprendre ou recommencer une session existante
app.get('/train/resume', requireAuth, (req, res) => {
  const state = req.session.trainState;
  if (!state) return res.redirect('/train/setup');
  const choice = req.query.choice === 'restart' ? 'restart' : 'resume';
  state.modes = normalizeModes(state.modes || state.mode);
  if (choice === 'restart') {
    const total = state.total === 'all' ? 'all' : Number(state.total || 0);
    state.answered = 0;
    state.correct = 0;
    state.remaining = total;
    state.usedWordIds = [];
    state.usedCardIds = [];
    state.replay = null;
  }
  if (choice === 'resume') {
    const isCardSession = Array.isArray(state.set_ids) && state.set_ids.length > 0;
    if (isCardSession && Array.isArray(state.usedCardIds) && state.usedCardIds.length > 0) {
      const lastId = state.usedCardIds[state.usedCardIds.length - 1];
      state.replay = { type: 'card', id: lastId, questionMode: state.currentMode || (state.modes || [])[0] || MODE_FLASHCARDS };
    } else if (!isCardSession && Array.isArray(state.usedWordIds) && state.usedWordIds.length > 0) {
      const lastId = state.usedWordIds[state.usedWordIds.length - 1];
      state.replay = { type: 'word', id: lastId, questionMode: state.currentMode || (state.modes || [])[0] || MODE_FLASHCARDS };
    } else {
      state.replay = null;
    }
  }
  req.session.trainState = state;
  return res.redirect(`/train`);
});

// Supprimer la session en cours et nettoyer les stats associées
app.post('/train/session/clear', requireAuth, async (req, res) => {
  const state = req.session.trainState;
  const userId = req.session.user.id;
  if (state) {
    try {
      if (state.theme_ids && state.theme_ids.length > 0) {
        const ph = state.theme_ids.map(() => '?').join(',');
        const wordIds = await all(`SELECT id FROM words WHERE theme_id IN (${ph})`, state.theme_ids);
        const ids = wordIds.map(w => w.id);
        if (ids.length > 0) {
          const phw = ids.map(() => '?').join(',');
          await run(`DELETE FROM progress WHERE user_id = ? AND word_id IN (${phw})`, [userId, ...ids]);
        }
      }
      if (state.set_ids && state.set_ids.length > 0) {
        const ph = state.set_ids.map(() => '?').join(',');
        const cardIds = await all(`SELECT id FROM cards WHERE set_id IN (${ph})`, state.set_ids);
        const cids = cardIds.map(c => c.id);
        if (cids.length > 0) {
          const phc = cids.map(() => '?').join(',');
          await run(`DELETE FROM card_progress WHERE user_id = ? AND card_id IN (${phc})`, [userId, ...cids]);
        }
      }
    } catch (e) {
      console.error('clear session error', e);
    }
    delete req.session.trainState;
  }
  res.redirect('/app');
});

// Toggle a word globally (admin or owner)
app.post('/words/:id/toggle', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const isAdmin = req.session.user && req.session.user.role === 'admin';
  const redirectTo = req.body.redirectTo || req.get('referer') || '/themes';
  try {
    const word = await get('SELECT * FROM words WHERE id = ?', [req.params.id]);
    if (!word) return res.redirect(redirectTo);
    if (!isAdmin && word.user_id !== userId) return res.status(403).send('Accès refusé');
    const newStatus = word.active ? 0 : 1;
    await run('UPDATE words SET active = ? WHERE id = ?', [newStatus, word.id]);
    if (newStatus === 1) {
      await run('DELETE FROM user_word_overrides WHERE word_id = ?', [word.id]);
    }
    res.redirect(redirectTo);
  } catch (e) {
    console.error(e);
    res.redirect(redirectTo);
  }
});

// Toggle a word for current user only (override)
app.post('/words/:id/toggle-self', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const redirectTo = req.body.redirectTo || req.get('referer') || '/themes';
  try {
    const word = await get('SELECT * FROM words WHERE id = ?', [req.params.id]);
    if (!word) return res.redirect(redirectTo);
    if (word.active === 0) {
      await run('DELETE FROM user_word_overrides WHERE user_id = ? AND word_id = ?', [userId, word.id]);
      return res.redirect(redirectTo);
    }
    const override = await get('SELECT * FROM user_word_overrides WHERE user_id = ? AND word_id = ?', [userId, word.id]);
    if (override && override.active === 0) {
      await run('DELETE FROM user_word_overrides WHERE user_id = ? AND word_id = ?', [userId, word.id]);
    } else {
      await run('INSERT OR REPLACE INTO user_word_overrides (user_id, word_id, active) VALUES (?,?,0)', [userId, word.id]);
    }
    res.redirect(redirectTo);
  } catch (e) {
    console.error(e);
    res.redirect(redirectTo);
  }
});

app.post('/my/words/batch-toggle-active', requireAuth, express.json(), async (req, res) => {
  const userId = req.session.user.id;
  const isAdmin = req.session.user && req.session.user.role === 'admin';
  const { word_ids } = req.body;
  const wordIds = normalizeIds(word_ids);

  if (wordIds.length === 0) {
    return res.status(400).json({ success: false, error: 'Missing parameters' });
  }

  try {
    const placeholders = wordIds.map(() => '?').join(',');
    const words = await all(
      `SELECT w.id, w.active, w.user_id, o.active as override_active
       FROM words w
       LEFT JOIN user_word_overrides o ON o.word_id = w.id AND o.user_id = ?
       WHERE w.id IN (${placeholders})`,
      [userId, ...wordIds]
    );

    const toggled = [];
    for (const word of words) {
      const canEditGlobal = isAdmin && !word.user_id;
      const isOwner = word.user_id === userId;

      if (canEditGlobal || isOwner) {
        const newStatus = word.active ? 0 : 1;
        await run('UPDATE words SET active = ? WHERE id = ?', [newStatus, word.id]);
      } else {
        // Regular user on a global word or another user's word they don't own
        const effectiveStatus = word.override_active === null ? word.active : word.override_active;
        const newStatus = effectiveStatus ? 0 : 1;
        await run(
          'INSERT INTO user_word_overrides (word_id, user_id, active) VALUES (?, ?, ?) ON CONFLICT(word_id, user_id) DO UPDATE SET active = excluded.active',
          [word.id, userId, newStatus]
        );
      }
      toggled.push(word.id);
    }
    res.json({ success: true, toggled });
  } catch (e) {
    console.error('Batch word toggle error:', e);
    res.status(500).json({ success: false, error: 'Server error' });
  }
});

// ---------- Admin ----------
app.get('/admin', requireAdmin, async (req, res) => {
  try {
    const userCount = await get('SELECT COUNT(*) AS c FROM users');
    const wordCount = await get('SELECT COUNT(*) AS c FROM words');
    const themeCount = await get('SELECT COUNT(*) AS c FROM themes');
    res.render('admin/dashboard', {
      userCount: userCount.c,
      wordCount: wordCount.c,
      themeCount: themeCount.c
    });
  } catch (e) {
    console.error(e);
    res.render('admin/dashboard', { userCount: 0, wordCount: 0, themeCount: 0 });
  }
});

app.get('/admin/duplicates', requireAdmin, async (req, res) => {
  try {
    const themeWords = await all(
      `SELECT w.id, w.hebrew, w.french, w.transliteration,
              t.name AS theme_name,
              l.name AS level_name
         FROM words w
         LEFT JOIN themes t ON t.id = w.theme_id
         LEFT JOIN theme_levels l ON l.id = w.level_id
        WHERE w.user_id IS NULL`
    );

    const listCards = await all(
      `SELECT c.id, c.hebrew, c.french, c.transliteration,
              s.name AS set_name,
              u.display_name AS owner_name
         FROM cards c
         JOIN sets s ON s.id = c.set_id
         JOIN users u ON u.id = s.user_id`
    );

    const themeMap = new Map();
    const listMap = new Map();
    const globalMap = new Map();

    const addEntry = (map, key, item) => {
      if (!key) return;
      if (!map.has(key)) map.set(key, []);
      map.get(key).push(item);
    };

    const addGlobal = (key, entry) => {
      if (!key) return;
      if (!globalMap.has(key)) globalMap.set(key, []);
      globalMap.get(key).push(entry);
    };

    themeWords.forEach(w => {
      const key = duplicateKey(w.hebrew, w.french);
      const item = {
        key,
        id: w.id,
        hebrew: w.hebrew,
        french: w.french,
        transliteration: w.transliteration || '',
        theme: w.theme_name || 'Sans theme',
        level: w.level_name || ''
      };
      addEntry(themeMap, key, item);
      addGlobal(key, { ...item, source: 'theme' });
    });

    listCards.forEach(c => {
      const key = duplicateKey(c.hebrew, c.french);
      const item = {
        key,
        id: c.id,
        hebrew: c.hebrew,
        french: c.french,
        transliteration: c.transliteration || '',
        list: c.set_name || 'Sans liste',
        owner: c.owner_name || 'Inconnu'
      };
      addEntry(listMap, key, item);
      addGlobal(key, { ...item, source: 'list' });
    });

    const mapToDuplicates = (map) => {
      return Array.from(map.entries())
        .filter(([, arr]) => arr.length > 1)
        .map(([key, arr]) => ({
          key,
          count: arr.length,
          sample: arr[0],
          items: arr
        }))
        .sort((a, b) => String(a.sample.french || '').localeCompare(String(b.sample.french || ''), 'fr', { sensitivity: 'base' }));
    };

    const themeDuplicates = mapToDuplicates(themeMap);
    const listDuplicates = mapToDuplicates(listMap);

    const crossDuplicates = Array.from(globalMap.entries())
      .map(([key, entries]) => {
        const sources = new Set(entries.map(e => e.source));
        return { key, entries, sources, sample: entries[0] };
      })
      .filter(g => g.entries.length > 1 && g.sources.size > 1)
      .sort((a, b) => String(a.sample.french || '').localeCompare(String(b.sample.french || ''), 'fr', { sensitivity: 'base' }));

    res.render('admin/duplicates', {
      themeDuplicates,
      listDuplicates,
      crossDuplicates,
      stats: {
        theme: themeDuplicates.length,
        list: listDuplicates.length,
        cross: crossDuplicates.length
      }
    });
  } catch (e) {
    console.error('Duplicate scan error:', e);
    res.render('admin/duplicates', {
      themeDuplicates: [],
      listDuplicates: [],
      crossDuplicates: [],
      stats: { theme: 0, list: 0, cross: 0 }
    });
  }
});

app.get('/admin/users', requireAdmin, async (req, res) => {
  try {
    const users = await all(
      'SELECT id, email, display_name, first_name, last_name, level, role, created_at FROM users ORDER BY id ASC'
    );
    res.render('admin/users', { users });
  } catch (e) {
    console.error(e);
    res.render('admin/users', { users: [] });
  }
});

app.get('/admin/users/new', requireAdmin, (req, res) => {
  res.render('admin/user_form', { user: null, action: '/admin/users' });
});

app.post('/admin/users', requireAdmin, async (req, res) => {
  const email = (req.body.email || '').trim();
  const password = req.body.password || '';
  const firstName = (req.body.first_name || '').trim();
  const lastNameClean = (req.body.last_name || '').trim();
  const displayRaw = (req.body.display_name || '').trim();
  const level = (req.body.level || '').trim();
  const role = req.body.role || 'user';
  const displayNameValue = displayRaw || [firstName, lastNameClean].filter(Boolean).join(' ').trim();
  if (!email || !password || !displayNameValue || !role || !firstName || !level) {
    return res.redirect('/admin/users');
  }
  try {
    const hash = await bcrypt.hash(password, 10);
    await run(
      'INSERT INTO users (email, password_hash, password_plain, display_name, first_name, last_name, level, role) VALUES (?,?,?,?,?,?,?,?)',
      [email, hash, password, displayNameValue, firstName, lastNameClean, level, role]
    );
    res.redirect('/admin/users');
  } catch (e) {
    console.error(e);
    res.redirect('/admin/users');
  }
});

app.get('/admin/users/:id/edit', requireAdmin, async (req, res) => {
  try {
    const user = await get('SELECT * FROM users WHERE id = ?', [req.params.id]);
    if (!user) return res.redirect('/admin/users');
    res.render('admin/user_form', {
      user,
      action: `/admin/users/${user.id}?_method=PUT`
    });
  } catch (e) {
    console.error(e);
    res.redirect('/admin/users');
  }
});

app.put('/admin/users/:id', requireAdmin, async (req, res) => {
  const email = (req.body.email || '').trim();
  const password = req.body.password || '';
  const firstName = (req.body.first_name || '').trim();
  const lastNameClean = (req.body.last_name || '').trim();
  const displayRaw = (req.body.display_name || '').trim();
  const level = (req.body.level || '').trim();
  const role = req.body.role || 'user';
  const displayNameValue = displayRaw || [firstName, lastNameClean].filter(Boolean).join(' ').trim();
  if (!email || !firstName || !displayNameValue || !role || !level) {
    return res.redirect('/admin/users');
  }
  try {
    if (password) {
      const hash = await bcrypt.hash(password, 10);
      await run(
        `UPDATE users SET email = ?, display_name = ?, role = ?, first_name = ?, last_name = ?, level = ?, password_hash = ?, password_plain = ? WHERE id = ?`,
        [email, displayNameValue, role, firstName, lastNameClean, level, hash, password, req.params.id]
      );
    } else {
      await run(
        `UPDATE users SET email = ?, display_name = ?, role = ?, first_name = ?, last_name = ?, level = ? WHERE id = ?`,
        [email, displayNameValue, role, firstName, lastNameClean, level, req.params.id]
      );
    }
    res.redirect('/admin/users');
  } catch (e) {
    console.error(e);
    res.redirect('/admin/users');
  }
});

app.delete('/admin/users/:id', requireAdmin, async (req, res) => {
  try {
    await run('DELETE FROM favorites WHERE user_id = ?', [req.params.id]);
    await run('DELETE FROM user_word_overrides WHERE user_id = ?', [req.params.id]);
    await run('DELETE FROM user_set_overrides WHERE user_id = ?', [req.params.id]);
    await run('DELETE FROM user_theme_overrides WHERE user_id = ?', [req.params.id]);
    await run('DELETE FROM card_overrides WHERE user_id = ?', [req.params.id]);
    await run('DELETE FROM set_shares WHERE user_id = ?', [req.params.id]);
    await run('DELETE FROM progress WHERE user_id = ?', [req.params.id]);
    await run('DELETE FROM card_progress WHERE user_id = ?', [req.params.id]);
    await run('DELETE FROM users WHERE id = ?', [req.params.id]);
    res.redirect('/admin/users');
  } catch (e) {
    console.error(e);
    res.redirect('/admin/users');
  }
});

app.post('/admin/users/:id/reset', requireAdmin, async (req, res) => {
  try {
    await run('DELETE FROM progress WHERE user_id = ?', [req.params.id]);
    await run('DELETE FROM card_progress WHERE user_id = ?', [req.params.id]);
    res.redirect('/admin/users');
  } catch (e) {
    console.error(e);
    res.redirect('/admin/users');
  }
});

app.get('/admin/themes', requireAdmin, async (req, res) => {
  try {
    const sort = req.query.sort || 'created_asc';
    let orderBy = 'COALESCE(t.created_at, t.id) ASC';
    if (sort === 'created_desc') orderBy = 'COALESCE(t.created_at, t.id) DESC';
    if (sort === 'alpha') orderBy = 't.name COLLATE NOCASE ASC';
    const themes = await all(
      `SELECT t.*,
        p.name AS parent_name,
        COUNT(w.id) AS word_count
       FROM themes t
       LEFT JOIN themes p ON p.id = t.parent_id
       LEFT JOIN words w ON w.theme_id = t.id AND w.user_id IS NULL
       WHERE t.user_id IS NULL
       GROUP BY t.id
       ORDER BY ${orderBy}`
    );

    // Build hierarchy for admin view
    const themeMap = new Map();
    const rootThemes = [];

    // First pass: create nodes
    themes.forEach(t => {
      t.children = [];
      themeMap.set(t.id, t);
    });

    // Second pass: link parents and children
    themes.forEach(t => {
      if (t.parent_id && themeMap.has(t.parent_id)) {
        themeMap.get(t.parent_id).children.push(t);
      } else {
        rootThemes.push(t);
      }
    });

    res.render('admin/themes', { themes: rootThemes, sort });
  } catch (e) {
    console.error(e);
    res.render('admin/themes', { themes: [], sort: req.query.sort || 'created_asc' });
  }
});


app.get('/admin/themes/new', requireAdmin, async (req, res) => {
  try {
    const potentialParents = await all('SELECT * FROM themes WHERE user_id IS NULL AND parent_id IS NULL ORDER BY name ASC');
    res.render('admin/theme_form', {
      theme: null,
      cards: [],
      potentialParents,
      action: '/admin/themes'
    });
  } catch (e) {
    console.error(e);
    res.redirect('/admin/themes');
  }
});

app.post('/admin/themes', requireAdmin, async (req, res) => {
  const { name, active, parent_id } = req.body;
  const cards = normalizeCardsPayload(req.body.cards);
  try {
    const displayNo = await nextDisplayNo('themes', null);
    const parentIdVal = parent_id ? Number(parent_id) : null;
    const newId = await findFirstAvailableId('themes');

    await run(
      'INSERT INTO themes (id, name, active, user_id, created_at, display_no, parent_id) VALUES (?,?,?,NULL, CURRENT_TIMESTAMP, ?, ?)',
      [newId, name, active ? 1 : 0, displayNo, parentIdVal]
    );
    const createdTheme = { lastID: newId };
    const newThemeId = createdTheme.lastID;
    const allocateWordId = cards.length > 0 ? await createIdAllocator('words') : null;
    if (newThemeId && cards.length > 0) {
      for (let i = 0; i < cards.length; i++) {
        const card = cards[i];
        const position = i + 1;
        const wordId = allocateWordId();
        await run(
          `INSERT INTO words (id, hebrew, transliteration, french, theme_id, level_id, difficulty, active, user_id, position)
           VALUES (?,?,?,?,?,?,?,?,?,?)`,
          [wordId, card.hebrew, card.transliteration || null, card.french, newThemeId, null, 1, 1, null, position]
        );
      }
    }
    res.redirect('/admin/themes');
  } catch (e) {
    console.error(e);
    res.redirect('/admin/themes');
  }
});

app.get('/admin/themes/:id/edit', requireAdmin, async (req, res) => {
  try {
    const theme = await get('SELECT * FROM themes WHERE id = ? AND user_id IS NULL', [req.params.id]);
    if (!theme) return res.redirect('/admin/themes');
    const cards = await all(
      'SELECT id, hebrew, french, transliteration FROM words WHERE theme_id = ? AND user_id IS NULL ORDER BY position ASC, id ASC',
      [theme.id]
    );
    const potentialParents = await all('SELECT * FROM themes WHERE user_id IS NULL AND parent_id IS NULL AND id != ? ORDER BY name ASC', [theme.id]);
    res.render('admin/theme_form', {
      theme,
      cards,
      potentialParents,
      action: `/admin/themes/${theme.id}?_method=PUT`
    });
  } catch (e) {
    console.error(e);
    res.redirect('/admin/themes');
  }
});

app.post('/admin/themes/:id/toggle', requireAdmin, async (req, res) => {
  try {
    const theme = await get('SELECT * FROM themes WHERE id = ? AND user_id IS NULL', [req.params.id]);
    if (!theme) return res.redirect('/admin/themes');
    const newStatus = theme.active ? 0 : 1;
    await run('UPDATE themes SET active = ? WHERE id = ?', [newStatus, theme.id]);

    // Cascade to direct children when toggling a parent.
    if (!theme.parent_id) {
      await run('UPDATE themes SET active = ? WHERE parent_id = ?', [newStatus, theme.id]);
    }

    res.redirect('/admin/themes');
  } catch (e) {
    console.error(e);
    res.redirect('/admin/themes');
  }
});

// Toggle theme active (owner or admin)
app.post('/themes/:id/toggle', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const isAdmin = req.session.user && req.session.user.role === 'admin';
  try {
    const theme = await get('SELECT * FROM themes WHERE id = ?', [req.params.id]);
    if (!theme) return res.redirect('/themes');
    if (theme.user_id && theme.user_id !== userId && !isAdmin) return res.status(403).send('AccÃ¨s refusÃ©');
    if (!theme.user_id && !isAdmin) return res.status(403).send('AccÃ¨s refusÃ©');
    const newStatus = theme.active ? 0 : 1;
    await run('UPDATE themes SET active = ? WHERE id = ?', [newStatus, theme.id]);
    res.redirect('/themes');
  } catch (e) {
    console.error(e);
    res.redirect('/themes');
  }
});

// Toggle level active (owner or admin)
app.post('/levels/:id/toggle', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const isAdmin = req.session.user && req.session.user.role === 'admin';
  try {
    const level = await get('SELECT * FROM theme_levels WHERE id = ?', [req.params.id]);
    if (!level) return res.redirect('/themes');
    const theme = await get('SELECT * FROM themes WHERE id = ?', [level.theme_id]);
    if (!theme) return res.redirect('/themes');
    if (theme.user_id && theme.user_id !== userId && !isAdmin) return res.status(403).send('AccÃ¨s refusÃ©');
    if (!theme.user_id && !isAdmin) return res.status(403).send('AccÃ¨s refusÃ©');
    const newStatus = level.active ? 0 : 1;
    await run('UPDATE theme_levels SET active = ? WHERE id = ?', [newStatus, level.id]);
    res.redirect(`/themes/${theme.id}`);
  } catch (e) {
    console.error(e);
    res.redirect('/themes');
  }
});

app.get('/admin/themes/:id', requireAdmin, async (req, res) => {
  try {
    const theme = await get(
      `SELECT t.*
       FROM themes t
       WHERE t.id = ? AND t.user_id IS NULL`,
      [req.params.id]
    );
    if (!theme) return res.redirect('/admin/themes');
    const words = await all(
      'SELECT id, hebrew, french, transliteration, active FROM words WHERE theme_id = ? AND user_id IS NULL ORDER BY position ASC, id ASC',
      [theme.id]
    );
    const targetThemes = await all(
      'SELECT id, name FROM themes WHERE user_id IS NULL AND id != ? ORDER BY name ASC',
      [theme.id]
    );
    res.render('admin/theme_show', { theme, words, targetThemes });
  } catch (e) {
    console.error(e);
    res.redirect('/admin/themes');
  }
});

app.post('/admin/themes/:themeId/words/:wordId/action', requireAdmin, async (req, res) => {
  const { action } = req.body;
  const redirectTo = `/admin/themes/${req.params.themeId}`;
  try {
    const word = await get('SELECT * FROM words WHERE id = ? AND theme_id = ? AND user_id IS NULL', [req.params.wordId, req.params.themeId]);
    if (!word) return res.redirect(redirectTo);
    if (action === 'toggle_active') {
      await run('UPDATE words SET active = ? WHERE id = ?', [word.active ? 0 : 1, word.id]);
    } else if (action === 'delete') {
      await run('DELETE FROM progress WHERE word_id = ?', [word.id]);
      await run('DELETE FROM favorites WHERE word_id = ?', [word.id]);
      await run('DELETE FROM words WHERE id = ?', [word.id]);
    }
  } catch (e) {
    console.error(e);
  }
  res.redirect(redirectTo);
});

app.post('/admin/themes/:id/move', requireAdmin, async (req, res) => {
  const sourceId = Number(req.params.id);
  const targetId = Number(req.body.target_theme_id);
  const wordIds = normalizeIds(req.body.word_ids);
  if (!targetId || wordIds.length === 0) return res.redirect(`/admin/themes/${sourceId}`);
  try {
    const source = await get('SELECT * FROM themes WHERE id = ? AND user_id IS NULL', [sourceId]);
    if (!source) return res.redirect('/admin/themes');
    const target = await get('SELECT * FROM themes WHERE id = ? AND user_id IS NULL', [targetId]);
    if (!target || target.id === source.id) return res.redirect(`/admin/themes/${source.id}`);

    const placeholders = wordIds.map(() => '?').join(',');
    const owned = await all(
      `SELECT id FROM words WHERE id IN (${placeholders}) AND theme_id = ? AND user_id IS NULL`,
      [...wordIds, source.id]
    );
    if (owned.length === 0) return res.redirect(`/admin/themes/${source.id}`);

    const ownedSet = new Set(owned.map(w => w.id));
    for (const wordId of wordIds) {
      if (!ownedSet.has(wordId)) continue;
      await run('UPDATE words SET theme_id = ?, level_id = NULL WHERE id = ?', [target.id, wordId]);
    }

    res.redirect(`/admin/themes/${target.id}`);
  } catch (e) {
    console.error('Move theme words error:', e);
    res.redirect(`/admin/themes/${sourceId}`);
  }
});

app.post('/admin/themes/:id/levels', requireAdmin, async (req, res) => {
  const { name } = req.body;
  const themeId = req.params.id;
  try {
    const orderRow = await get('SELECT MAX(level_order) AS max_order FROM theme_levels WHERE theme_id = ?', [themeId]);
    const nextOrder = (orderRow && orderRow.max_order ? orderRow.max_order : 0) + 1;
    const levelName = name && name.trim() ? name.trim() : `Niveau ${nextOrder}`;
    await run('INSERT INTO theme_levels (theme_id, name, level_order, active) VALUES (?,?,?,1)', [themeId, levelName, nextOrder]);
    res.redirect(`/admin/themes/${themeId}`);
  } catch (e) {
    console.error(e);
    res.redirect('/admin/themes');
  }
});

app.put('/admin/themes/:id', requireAdmin, async (req, res) => {
  const { name, active, parent_id } = req.body;
  const cards = normalizeCardsPayload(req.body.cards);
  try {
    const theme = await get('SELECT * FROM themes WHERE id = ? AND user_id IS NULL', [req.params.id]);
    if (!theme) return res.redirect('/admin/themes');

    const parentIdVal = parent_id ? Number(parent_id) : null;
    // Prevent self-parenting loop (basic check)
    if (parentIdVal === theme.id) {
      // ignore or error
    }

    await run(
      'UPDATE themes SET name = ?, active = ?, parent_id = ? WHERE id = ?',
      [name, active ? 1 : 0, parentIdVal, theme.id]
    );

    const existing = await all('SELECT * FROM words WHERE theme_id = ? AND user_id IS NULL', [theme.id]);
    const existingById = new Map(existing.map(w => [Number(w.id), w]));
    const cardIds = new Set(cards.map(c => (c.id ? Number(c.id) : null)).filter(Boolean));

    for (const w of existing) {
      const wId = Number(w.id);
      if (!cardIds.has(wId)) {
        await run('DELETE FROM progress WHERE word_id = ?', [w.id]);
        await run('DELETE FROM favorites WHERE word_id = ?', [w.id]);
        await run('DELETE FROM words WHERE id = ?', [w.id]);
        existingById.delete(wId);
      }
    }

    const allocateWordId = await createIdAllocator('words');

    for (let i = 0; i < cards.length; i++) {
      const card = cards[i];
      const position = i + 1;
      const cardId = card.id ? Number(card.id) : null;
      if (cardId && existingById.has(cardId)) {
        const current = existingById.get(cardId);
        await run(
          'UPDATE words SET hebrew = ?, french = ?, transliteration = ?, position = ? WHERE id = ? AND theme_id = ? AND user_id IS NULL',
          [card.hebrew, card.french, card.transliteration || null, position, current.id, theme.id]
        );
      } else {
        const wordId = allocateWordId();
        await run(
          `INSERT INTO words (id, hebrew, transliteration, french, theme_id, level_id, difficulty, active, user_id, position)
           VALUES (?,?,?,?,?,?,?,?,?,?)`,
          [wordId, card.hebrew, card.transliteration || null, card.french, theme.id, null, 1, 1, null, position]
        );
      }
    }

    res.redirect('/admin/themes');
  } catch (e) {
    console.error(e);
    res.redirect('/admin/themes');
  }
});

app.delete('/admin/levels/:id', requireAdmin, async (req, res) => {
  try {
    const level = await get('SELECT * FROM theme_levels WHERE id = ?', [req.params.id]);
    if (!level) return res.redirect('/admin/themes');
    await run('UPDATE words SET level_id = NULL WHERE level_id = ?', [level.id]);
    await run('DELETE FROM theme_levels WHERE id = ?', [level.id]);
    res.redirect(`/admin/themes/${level.theme_id}`);
  } catch (e) {
    console.error(e);
    res.redirect('/admin/themes');
  }
});

app.post('/admin/levels/:id/toggle', requireAdmin, async (req, res) => {
  try {
    const level = await get('SELECT * FROM theme_levels WHERE id = ?', [req.params.id]);
    if (!level) return res.redirect('/admin/themes');
    const newStatus = level.active ? 0 : 1;
    await run('UPDATE theme_levels SET active = ? WHERE id = ?', [newStatus, level.id]);
    res.redirect(`/admin/themes/${level.theme_id}`);
  } catch (e) {
    console.error(e);
    res.redirect('/admin/themes');
  }
});

app.delete('/admin/themes/:id', requireAdmin, async (req, res) => {
  try {
    // Cascading delete: delete sub-themes first
    // For each sub-theme, we need to clean up its words/levels/progress too
    const subThemes = await all('SELECT id FROM themes WHERE parent_id = ?', [req.params.id]);
    for (const sub of subThemes) {
      await run('DELETE FROM progress WHERE word_id IN (SELECT id FROM words WHERE theme_id = ? AND user_id IS NULL)', [sub.id]);
      await run('DELETE FROM favorites WHERE word_id IN (SELECT id FROM words WHERE theme_id = ? AND user_id IS NULL)', [sub.id]);
      await run('DELETE FROM words WHERE theme_id = ? AND user_id IS NULL', [sub.id]);
      await run('DELETE FROM theme_levels WHERE theme_id = ?', [sub.id]);
      await run('DELETE FROM themes WHERE id = ?', [sub.id]);
    }

    // Now delete the theme itself
    await run('DELETE FROM progress WHERE word_id IN (SELECT id FROM words WHERE theme_id = ? AND user_id IS NULL)', [req.params.id]);
    await run('DELETE FROM favorites WHERE word_id IN (SELECT id FROM words WHERE theme_id = ? AND user_id IS NULL)', [req.params.id]);
    await run('DELETE FROM words WHERE theme_id = ? AND user_id IS NULL', [req.params.id]);
    await run('DELETE FROM theme_levels WHERE theme_id = ?', [req.params.id]);
    await run('DELETE FROM themes WHERE id = ? AND user_id IS NULL', [req.params.id]);
    await renumberThemes(null);
    res.redirect('/admin/themes');
  } catch (e) {
    console.error(e);
    res.redirect('/admin/themes');
  }
});

app.get('/admin/words', requireAdmin, async (req, res) => {
  try {
    const { search, theme_id, level_id, active, difficulty, sort } = req.query;
    const params = [];
    let sql = `
      SELECT w.*, t.name AS theme_name, l.name AS level_name
      FROM words w
      LEFT JOIN themes t ON t.id = w.theme_id
      LEFT JOIN theme_levels l ON l.id = w.level_id
      WHERE w.user_id IS NULL
    `;
    if (theme_id) {
      sql += ' AND w.theme_id = ?';
      params.push(theme_id);
    }
    if (level_id) {
      sql += ' AND w.level_id = ?';
      params.push(level_id);
    }
    if (active === '1' || active === '0') {
      sql += ' AND w.active = ?';
      params.push(active);
    }
    if (difficulty && ['1', '2', '3'].includes(String(difficulty))) {
      sql += ' AND w.difficulty = ?';
      params.push(difficulty);
    }
    if (search && search.trim()) {
      sql += ' AND (w.french LIKE ? OR w.hebrew LIKE ? OR w.transliteration LIKE ?)';
      const like = `%${search.trim()}%`;
      params.push(like, like, like);
    }
    const sortMap = {
      id: 'w.id ASC',
      french: 'w.french ASC',
      difficulty: 'w.difficulty ASC',
      recent: 'w.id DESC'
    };
    sql += ' ORDER BY ' + (sortMap[sort] || 'w.id ASC');

    const words = await all(sql, params);
    const themes = await all('SELECT * FROM themes ORDER BY id ASC');
    const levels = await all(
      `SELECT tl.*, t.name AS theme_name
       FROM theme_levels tl
       JOIN themes t ON t.id = tl.theme_id
       ORDER BY t.id ASC, tl.level_order, tl.id`
    );
    res.render('admin/words', {
      words,
      filters: { search, theme_id, level_id, active, difficulty, sort },
      themes,
      levels
    });
  } catch (e) {
    console.error(e);
    res.render('admin/words', { words: [], filters: {}, themes: [], levels: [] });
  }
});

app.get('/admin/words/new', requireAdmin, async (req, res) => {
  try {
    const themes = await all('SELECT * FROM themes ORDER BY id ASC');
    const levels = await all(
      `SELECT tl.*, t.name AS theme_name
       FROM theme_levels tl
       JOIN themes t ON t.id = tl.theme_id
       ORDER BY t.id ASC, tl.level_order, tl.id`
    );
    res.render('admin/word_form', {
      word: null,
      themes,
      levels,
      action: '/admin/words'
    });
  } catch (e) {
    console.error(e);
    res.redirect('/admin/words');
  }
});

app.post('/admin/words', requireAdmin, async (req, res) => {
  const { hebrew, transliteration, french, theme_id, level_id, difficulty, active } = req.body;
  try {
    const { levelId, themeId } = await normalizeLevelSelection(theme_id, level_id);
    const wordId = await findFirstAvailableId('words');
    const position = await nextWordPosition(themeId || null, null);
    await run(
      `INSERT INTO words (id, hebrew, transliteration, french, theme_id, level_id, difficulty, active, user_id, position)
       VALUES (?,?,?,?,?,?,?,?,?,?)`,
      [wordId, hebrew, transliteration || null, french, themeId || null, levelId, difficulty || 1, active ? 1 : 0, null, position]
    );
    res.redirect('/admin/words');
  } catch (e) {
    console.error(e);
    res.redirect('/admin/words');
  }
});

app.get('/admin/words/:id/edit', requireAdmin, async (req, res) => {
  try {
    const word = await get('SELECT * FROM words WHERE id = ?', [req.params.id]);
    if (!word) return res.redirect('/admin/words');
    const themes = await all('SELECT * FROM themes ORDER BY id ASC');
    const levels = await all(
      `SELECT tl.*, t.name AS theme_name
       FROM theme_levels tl
       JOIN themes t ON t.id = tl.theme_id
       ORDER BY t.id ASC, tl.level_order, tl.id`
    );
    res.render('admin/word_form', {
      word,
      themes,
      levels,
      action: `/admin/words/${word.id}?_method=PUT`
    });
  } catch (e) {
    console.error(e);
    res.redirect('/admin/words');
  }
});

app.put('/admin/words/:id', requireAdmin, async (req, res) => {
  const { hebrew, transliteration, french, theme_id, level_id, difficulty, active } = req.body;
  try {
    const { levelId, themeId } = await normalizeLevelSelection(theme_id, level_id);
    await run(
      `UPDATE words
       SET hebrew = ?, transliteration = ?, french = ?, theme_id = ?, level_id = ?, difficulty = ?, active = ?
       WHERE id = ?`,
      [
        hebrew,
        transliteration || null,
        french,
        themeId || null,
        levelId,
        difficulty || 1,
        active ? 1 : 0,
        req.params.id
      ]
    );
    res.redirect('/admin/words');
  } catch (e) {
    console.error(e);
    res.redirect('/admin/words');
  }
});

app.delete('/admin/words/:id', requireAdmin, async (req, res) => {
  try {
    await run('DELETE FROM progress WHERE word_id = ?', [req.params.id]);
    await run('DELETE FROM favorites WHERE word_id = ?', [req.params.id]);
    await run('DELETE FROM words WHERE id = ?', [req.params.id]);
    res.redirect('/admin/words');
  } catch (e) {
    console.error(e);
    res.redirect('/admin/words');
  }
});

// Lancement du serveur
app.listen(PORT, () => {
  console.log(`Server started at http://localhost:${PORT}`);
});












































































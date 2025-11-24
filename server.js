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

const MODE_FLASHCARDS = 'flashcards';
const MODE_FLASHCARDS_REVERSE = 'flashcards_reverse';
const MODE_WRITTEN = 'written';
const ALLOWED_MODES = [MODE_FLASHCARDS, MODE_FLASHCARDS_REVERSE, MODE_WRITTEN];

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
      return 'Revision par ecrit';
    default:
      return 'Revision';
  }
}

async function fetchNextWord(userId, filters = {}) {
  const mode = (filters.rev_mode || 'weak').toLowerCase();
  const params = [userId, userId, userId];
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
       LEFT JOIN theme_levels l ON l.id = w.level_id
       WHERE w.active = 1
         AND COALESCE(uwo.active, 1) = 1
         AND (w.theme_id IS NULL OR t.active = 1)
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
  const params = [userId, userId, currentWord.id];
  let sql = `SELECT w.id, w.french, w.hebrew
    FROM words w
    LEFT JOIN progress p ON p.word_id = w.id AND p.user_id = ?
    LEFT JOIN user_word_overrides uwo ON uwo.word_id = w.id AND uwo.user_id = ?
    LEFT JOIN themes t ON t.id = w.theme_id
    LEFT JOIN theme_levels l ON l.id = w.level_id
    WHERE w.active = 1 AND w.id != ?
      AND COALESCE(uwo.active, 1) = 1
      AND (w.theme_id IS NULL OR t.active = 1)
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
     WHERE (t.user_id IS NULL OR t.user_id = ?) AND t.active = 1 AND tl.active = 1
     ORDER BY t.id ASC, tl.level_order, tl.id`,
    [userId]
  );
}

async function getOwnedLevels(userId) {
  return all(
    `SELECT tl.*, t.name AS theme_name, t.user_id AS theme_user_id
     FROM theme_levels tl
     JOIN themes t ON t.id = tl.theme_id
     WHERE t.user_id = ? AND t.active = 1 AND tl.active = 1
     ORDER BY t.id ASC, tl.level_order, tl.id`,
    [userId]
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

function requireAuth(req, res, next) {
  if (!req.session.user) return res.redirect('/login');
  next();
}

function requireAdmin(req, res, next) {
  if (!req.session.user || req.session.user.role !== 'admin') {
    return res.status(403).send('AccÃ¨s refusÃ©');
  }
  next();
}

// Admin par dÃ©faut
async function ensureAdmin() {
  const admin = await get("SELECT * FROM users WHERE role = 'admin'");
  if (!admin) {
    const hash = await bcrypt.hash('admin123', 10);
    await run(
      'INSERT INTO users (email, password_hash, display_name, role) VALUES (?,?,?,?)',
      ['admin@example.com', hash, 'Admin', 'admin']
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
    for (const name of seedThemes) {
      const created = await run('INSERT INTO themes (name, active, created_at) VALUES (?,1, CURRENT_TIMESTAMP)', [name]);
      await run('INSERT INTO theme_levels (theme_id, name, level_order, active) VALUES (?,?,?,1)', [created.lastID, 'Niveau 1', 1]);
    }
    console.log('ThÃ¨mes de base crÃ©Ã©s (liste fournie).');
  }
}
(async () => {
  try {
    await ensureAdmin();
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
  const { email, password, display_name } = req.body;
  if (!email || !password || !display_name) {
    return res.render('register', { error: 'Tous les champs sont obligatoires.' });
  }
  try {
    const existing = await get('SELECT * FROM users WHERE email = ?', [email]);
    if (existing) {
      return res.render('register', { error: 'Cet email est dÃ©jÃ  utilisÃ©.' });
    }
    const hash = await bcrypt.hash(password, 10);
    const info = await run(
      'INSERT INTO users (email, password_hash, display_name, role) VALUES (?,?,?,?)',
      [email, hash, display_name, 'user']
    );
    req.session.user = {
      id: info.lastID,
      email,
      display_name,
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
        COUNT(DISTINCT card_id) AS cards_seen,
        SUM(success_count) AS total_success,
        SUM(fail_count) AS total_fail
       FROM card_progress WHERE user_id = ?`,
      [userId]
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
    if (state && Number(state.answered || 0) > 0) {
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
    const weakWords = await all(
      `SELECT w.*, p.strength
       FROM progress p
       JOIN words w ON w.id = p.word_id
       WHERE p.user_id = ?
       ORDER BY p.strength ASC
       LIMIT 10`,
      [userId]
    );
    res.render('profile', { user, stats, weakWords });
  } catch (e) {
    console.error(e);
    res.render('profile', { user: req.session.user, stats: null, weakWords: [] });
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
    const themes = await all('SELECT * FROM themes WHERE user_id = ? AND active = 1 ORDER BY id ASC', [userId]);
    res.render('my_words', { globalWords: [], personalWords, favorites, themes, listFavorites });
  } catch (e) {
    console.error(e);
    res.render('my_words', { globalWords: [], personalWords: [], favorites: [], themes: [], listFavorites: [] });
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
  const filters = {
    theme_id: state.theme_id || null,
    theme_ids: state.theme_ids || [],
    level_id: state.level_id || null,
    difficulty: state.difficulty || '',
    rev_mode: state.rev_mode || 'random',
    scope: state.scope || 'all',
    scope_list: [],
    source: state.set_ids && state.set_ids.length > 0 ? 'cards' : 'words',
    set_ids: state.set_ids || []
  };

  const remainingCount = Math.max(0, Number(state.remaining || 1) - 1);
  const totalCount = Number(state.total || state.remaining || 1);
  const answeredCount = Number(state.answered || (totalCount - remainingCount - 1)) + 1;
  let correctCount = Number(state.correct || 0);

  try {
    if (filters.source === 'cards') {
      if (!String(word_id).startsWith('card_')) return res.redirect('/train');
      const cardId = Number(String(word_id).replace('card_', ''));
      const chosenId = String(choice_word_id).replace('card_', '');
      const card = await get('SELECT * FROM cards WHERE id = ?', [cardId]);
      if (!card) return res.redirect('/train');
      const isCorrect = String(chosenId) === String(cardId);
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
      return res.render('train', {
        word: null,
        message: remainingCount > 0 ? null : 'Session terminee.',
        result,
        mode: modeList[0] || activeMode,
        modes: modeList,
        questionMode: activeMode,
        filters,
        options: null,
        themes: await all('SELECT * FROM themes WHERE (user_id IS NULL OR user_id = ?) AND active = 1 ORDER BY id ASC', [userId]),
        remaining: remainingCount,
        total: totalCount,
        answered: answeredCount,
        correct: correctCount,
        nextUrl: remainingCount > 0 ? '/train' : null
      });
    } else {
      const word = await get('SELECT * FROM words WHERE id = ?', [word_id]);
      if (!word) return res.redirect('/train');
      const isCorrect = String(choice_word_id) === String(word_id);
      if (isCorrect) correctCount += 1;
      await upsertProgress(userId, word_id, isCorrect);
      if (remainingCount > 0) {
        req.session.trainState = { ...state, correct: correctCount, answered: answeredCount, remaining: remainingCount, total: totalCount, modes: modeList };
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

      res.render('train', {
        word: null,
        message: remainingCount > 0 ? null : 'Session terminee.',
        result,
        mode: modeList[0] || activeMode,
        modes: modeList,
        questionMode: activeMode,
        filters,
        options: null,
        themes: await all('SELECT * FROM themes WHERE (user_id IS NULL OR user_id = ?) AND active = 1 ORDER BY id ASC', [userId]),
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
    await run(
      `INSERT INTO words (hebrew, transliteration, french, theme_id, level_id, difficulty, active, user_id)
       VALUES (?,?,?,?,?,?,?,?)`,
      [
        hebrew,
        transliteration || null,
        french,
        themeId || null,
        levelId,
        difficulty || 1,
        active ? 1 : 0,
        userId
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
  try {
    await run('DELETE FROM progress WHERE word_id = ? AND user_id = ?', [req.params.id, userId]);
    await run('DELETE FROM favorites WHERE word_id = ? AND user_id = ?', [req.params.id, userId]);
    await run('DELETE FROM words WHERE id = ? AND user_id = ?', [req.params.id, userId]);
    res.redirect('/my/words');
  } catch (e) {
    console.error(e);
    res.redirect('/my/words');
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
    const sets = await all(
      `SELECT s.*, COUNT(c.id) AS card_count
       FROM sets s
       LEFT JOIN cards c ON c.set_id = s.id AND c.active = 1
       WHERE s.user_id = ? AND s.active = 1
       GROUP BY s.id
       ORDER BY ${orderBy}`,
      [userId]
    );
    res.render('my_lists', { sets, pageClass: 'page-compact', sort });
  } catch (e) {
    console.error(e);
    res.render('my_lists', { sets: [], pageClass: 'page-compact', sort: req.query.sort || 'created_asc' });
  }
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
        COUNT(DISTINCT card_id) AS cards_seen,
        SUM(success_count) AS total_success,
        SUM(fail_count) AS total_fail
       FROM card_progress
       WHERE user_id = ?`,
      [userId]
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
      `SELECT s.id, s.name,
              COUNT(c.id) AS total_cards,
              SUM(CASE WHEN c.active = 1 THEN 1 ELSE 0 END) AS active_cards,
              SUM(CASE WHEN c.memorized = 1 THEN 1 ELSE 0 END) AS memorized_cards,
              SUM(CASE WHEN c.favorite = 1 THEN 1 ELSE 0 END) AS favorite_cards
       FROM sets s
       LEFT JOIN cards c ON c.set_id = s.id
       WHERE s.user_id = ? AND s.active = 1
       GROUP BY s.id
       ORDER BY s.created_at DESC`,
      [userId]
    );

    const themeStats = await all(
      `SELECT t.id, t.name,
              COUNT(w.id) AS total_words,
              SUM(CASE WHEN p.id IS NOT NULL THEN 1 ELSE 0 END) AS seen_words,
              AVG(p.strength) AS avg_strength
       FROM themes t
       LEFT JOIN words w ON w.theme_id = t.id AND w.active = 1
       LEFT JOIN progress p ON p.word_id = w.id AND p.user_id = ?
       WHERE t.user_id IS NULL AND t.active = 1
       GROUP BY t.id
       ORDER BY t.id ASC`,
      [userId]
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

app.get('/my/lists/new', requireAuth, (req, res) => {
  const defaultCards = [{ hebrew: '', french: '', transliteration: '' }, { hebrew: '', french: '', transliteration: '' }];
  res.render('my_list_form', {
    set: null,
    cards: defaultCards,
    action: '/my/lists',
    error: null
  });
});

app.get('/my/lists/:id', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  try {
    const set = await get('SELECT * FROM sets WHERE id = ? AND user_id = ?', [req.params.id, userId]);
    if (!set) return res.redirect('/my/lists');
    const cards = await all(
      'SELECT * FROM cards WHERE set_id = ? ORDER BY position ASC, id ASC',
      [set.id]
    );
    res.render('my_list_show', {
      set,
      cards
    });
  } catch (e) {
    console.error(e);
    res.redirect('/my/lists');
  }
});

app.get('/my/lists/:id/cards/:cardId/edit', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  try {
    const set = await get('SELECT * FROM sets WHERE id = ? AND user_id = ?', [req.params.id, userId]);
    if (!set) return res.redirect('/my/lists');
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
    const set = await get('SELECT * FROM sets WHERE id = ? AND user_id = ?', [req.params.id, userId]);
    if (!set) return res.redirect('/my/lists');
    const card = await get('SELECT * FROM cards WHERE id = ? AND set_id = ?', [req.params.cardId, set.id]);
    if (!card) return res.redirect(`/my/lists/${set.id}`);
    const { hebrew, french, transliteration, active, favorite, memorized } = req.body;
    if (!hebrew || !french) {
      return res.render('my_card_form', {
        set,
        card: { ...card, hebrew, french, transliteration, active: active ? 1 : 0, favorite: favorite ? 1 : 0, memorized: memorized ? 1 : 0 },
        action: `/my/lists/${set.id}/cards/${card.id}?_method=PUT`,
        error: 'Hebreu et franÃ§ais sont obligatoires.'
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
    const set = await get('SELECT * FROM sets WHERE id = ? AND user_id = ?', [req.params.id, userId]);
    if (!set) return res.redirect('/my/lists');
    const cards = await all(
      'SELECT * FROM cards WHERE set_id = ? ORDER BY position ASC, id ASC',
      [set.id]
    );
    res.render('my_list_form', {
      set,
      cards: cards.length > 0 ? cards : [{ hebrew: '', french: '', transliteration: '' }],
      action: `/my/lists/${set.id}?_method=PUT`,
      error: null
    });
  } catch (e) {
    console.error(e);
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
      cards: cards.length > 0 ? cards : [{ hebrew: '', french: '', transliteration: '' }],
      action: '/my/lists',
      error: 'Donne un titre Ã  ta liste.'
    });
  }
  if (cards.length === 0) {
    return res.render('my_list_form', {
      set: null,
      cards: [{ hebrew: '', french: '', transliteration: '' }],
      action: '/my/lists',
      error: 'Ajoute au moins une carte (hÃ©breu + franÃ§ais).'
    });
  }
  try {
    const info = await run(
      'INSERT INTO sets (name, user_id, active) VALUES (?,?,1)',
      [title, userId]
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
      cards: cards.length > 0 ? cards : [{ hebrew: '', french: '', transliteration: '' }],
      action: '/my/lists',
      error: 'Impossible de crÃ©er la liste pour le moment.'
    });
  }
});

app.put('/my/lists/:id', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const { name } = req.body;
  const cards = normalizeCardsPayload(req.body.cards);
  try {
    const set = await get('SELECT * FROM sets WHERE id = ? AND user_id = ?', [req.params.id, userId]);
    if (!set) return res.redirect('/my/lists');
    if (cards.length === 0) {
      return res.render('my_list_form', {
        set,
        cards: [{ hebrew: '', french: '', transliteration: '' }],
        action: `/my/lists/${set.id}?_method=PUT`,
        error: 'Ajoute au moins une carte (hÃ©breu + franÃ§ais).'
      });
    }
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

    for (const id of existingIds) {
      if (!keptIds.has(Number(id))) {
        await run('DELETE FROM cards WHERE id = ? AND set_id = ?', [id, set.id]);
      }
    }

    res.redirect('/my/lists');
  } catch (e) {
    console.error(e);
    res.render('my_list_form', {
      set: { id: req.params.id, name: name || '' },
      cards: cards.length > 0 ? cards : [{ hebrew: '', french: '', transliteration: '' }],
      action: `/my/lists/${req.params.id}?_method=PUT`,
      error: 'Impossible d\'enregistrer la liste.'
    });
  }
});

app.post('/my/lists/:id/cards/:cardId/action', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const { action, redirectTo } = req.body;
  const fallback = `/my/lists/${req.params.id}`;
  const allowedRedirects = ['/my/lists', '/my/words'];
  const backUrl = redirectTo && allowedRedirects.some(p => redirectTo.startsWith(p)) ? redirectTo : fallback;
  try {
    const set = await get('SELECT * FROM sets WHERE id = ? AND user_id = ?', [req.params.id, userId]);
    if (!set) return res.redirect('/my/lists');
    const card = await get('SELECT * FROM cards WHERE id = ? AND set_id = ?', [req.params.cardId, set.id]);
    if (!card) return res.redirect(backUrl);

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
  } catch (e) {
    console.error(e);
  }
  res.redirect(backUrl);
});

app.delete('/my/lists/:id', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  try {
    const set = await get('SELECT * FROM sets WHERE id = ? AND user_id = ?', [req.params.id, userId]);
    if (!set) return res.redirect('/my/lists');
    await run('DELETE FROM cards WHERE set_id = ?', [set.id]);
    await run('DELETE FROM sets WHERE id = ?', [set.id]);
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
    const proposedThemes = await all(
      `SELECT t.*,
        (SELECT COUNT(*) FROM theme_levels l WHERE l.theme_id = t.id) AS level_count
       FROM themes t
       WHERE t.user_id IS NULL AND t.active = 1
       ORDER BY t.id ASC`
    );
    const inactiveProposed = await all(
      `SELECT t.*
       FROM themes t
       WHERE t.user_id IS NULL AND t.active = 0
       ORDER BY t.id ASC`
    );

    const allIds = [...proposedThemes].map(t => t.id);
    let levelsByTheme = {};
    if (allIds.length > 0) {
      const placeholders = allIds.map(() => '?').join(',');
      const levels = await all(
        `SELECT * FROM theme_levels WHERE theme_id IN (${placeholders}) AND active = 1 ORDER BY level_order, id`,
        allIds
      );
      levelsByTheme = levels.reduce((acc, lvl) => {
        if (!acc[lvl.theme_id]) acc[lvl.theme_id] = [];
        acc[lvl.theme_id].push(lvl);
        return acc;
      }, {});
    }
    res.render('themes', {
      proposedThemes,
      inactiveProposed,
      personalThemes: [],
      inactivePersonal: [],
      levelsByTheme,
      pageClass: 'page-compact'
    });
  } catch (e) {
    console.error(e);
    res.render('themes', { proposedThemes: [], personalThemes: [], inactiveProposed: [], inactivePersonal: [], levelsByTheme: {}, pageClass: 'page-compact' });
  }
}

app.get('/themes', requireAuth, renderThemeList);
app.get('/my/themes', requireAuth, renderThemeList);

// ---------- Sets (Quizlet-like) ----------
app.get('/my/themes/new', requireAuth, async (req, res) => {
  res.render('my_theme_form', { theme: null, action: '/my/themes' });
});

app.post('/my/themes', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const { name } = req.body;
  if (!name) return res.redirect('/themes');
  try {
    await run('INSERT INTO themes (name, user_id, created_at) VALUES (?,?, CURRENT_TIMESTAMP)', [name, userId]);
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
    await run('DELETE FROM themes WHERE id = ? AND user_id = ?', [req.params.id, userId]);
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
      `SELECT t.*
       FROM themes t
       WHERE t.id = ?`,
      [req.params.id]
    );
    if (!theme || (theme.user_id && theme.user_id !== userId) || (!theme.active && !isAdmin)) {
      return res.redirect('/themes');
    }
    const words = await all(
      `SELECT w.id, w.hebrew, w.transliteration, w.french, w.active AS global_active, w.user_id,
        uwo.active AS override_active,
        CASE WHEN w.active = 0 THEN 0 ELSE COALESCE(uwo.active, w.active) END AS effective_active
       FROM words w
       LEFT JOIN user_word_overrides uwo ON uwo.word_id = w.id AND uwo.user_id = ?
       WHERE w.theme_id = ? AND (w.user_id IS NULL OR w.user_id = ?)
       ORDER BY w.id ASC`,
      [userId, theme.id, userId]
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
	  const themes = await all('SELECT * FROM themes WHERE (user_id IS NULL OR user_id = ?) AND active = 1 ORDER BY id ASC', [userId]);
	  const levels = await getLevelsForUser(userId);
	  const sets = await all('SELECT id, name FROM sets WHERE user_id = ? AND active = 1 ORDER BY created_at DESC', [userId]);
	  const state = req.session.trainState || null;
	  const useSessionParams = (req.query.current === '1' || req.query.current === 'true' || req.query.source === 'session');

  const baseParams = {
    modes: ['flashcards'],
    rev_mode: 'order',
    difficulty: '',
    theme_ids: [],
    set_ids: [],
    level_id: '',
    scope: 'all',
    remaining: 10,
    total: 10
  };

	  let params = { ...baseParams };
	  if (useSessionParams && state) {
	    const themeIds = Array.isArray(state.theme_ids) ? state.theme_ids : state.theme_ids ? [state.theme_ids] : [];
	    const setIds = Array.isArray(state.set_ids) ? state.set_ids : state.set_ids ? [state.set_ids] : [];
	    const remainingVal = state.remaining === null || typeof state.remaining === 'undefined' ? baseParams.remaining : state.remaining;
	    const totalVal = state.total === null || typeof state.total === 'undefined' ? remainingVal : state.total;
	    params = {
	      ...baseParams,
	      modes: normalizeModes(state.modes || state.mode),
      rev_mode: state.rev_mode || baseParams.rev_mode,
      difficulty: state.difficulty || baseParams.difficulty,
	      theme_ids: themeIds,
	      set_ids: setIds,
	      level_id: state.level_id || '',
	      scope: state.scope || baseParams.scope,
	      remaining: remainingVal,
	      total: totalVal
	    };
	  }

	  res.render('train_setup', {
	    themes,
	    levels,
	    sets,
	    params
	  });
	});

app.post('/train/session', requireAuth, async (req, res) => {
  const { modes, mode, theme_ids, set_ids, level_id, difficulty, rev_mode, remaining } = req.body;
  const rawModes = modes || req.body['modes[]'] || mode;
  const modeList = normalizeModes(rawModes);
  const themeRaw = Array.isArray(theme_ids) ? theme_ids.filter(Boolean) : theme_ids ? [theme_ids] : [];
  const setRaw = Array.isArray(set_ids) ? set_ids.filter(Boolean) : set_ids ? [set_ids] : [];
  const themeList = themeRaw.flatMap(t => String(t).split(',')).filter(Boolean);
  const setList = setRaw.flatMap(s => String(s).split(',')).filter(Boolean).map(Number).filter(Boolean);
  let remain = remaining === 'all' ? null : Number(remaining || 10);

  if (themeList.length === 0 && setList.length === 0) {
    const userId = req.session.user.id;
    const themes = await all('SELECT * FROM themes WHERE (user_id IS NULL OR user_id = ?) AND active = 1 ORDER BY id ASC', [userId]);
    const levels = await getLevelsForUser(userId);
    const sets = await all('SELECT id, name FROM sets WHERE user_id = ? AND active = 1 ORDER BY created_at DESC', [userId]);
    return res.render('train_setup', {
      themes,
      levels,
      sets,
      error: 'Selectionne au moins un theme ou une liste.',
      params: {
        modes: modeList,
      rev_mode: rev_mode || 'order',
        difficulty: difficulty || '',
        theme_id: '',
        level_id: level_id || '',
        scope: 'all',
        remaining: remain || 10,
        total: remain || 10
      }
    });
  }

  let poolWords = null;
  let poolCards = null;
  if (remain === null) {
    if (setList.length > 0) {
      const placeholders = setList.map(() => '?').join(',');
      poolCards = await all(
        `SELECT id FROM cards WHERE set_id IN (${placeholders}) AND active = 1 ORDER BY set_id ASC, position ASC, id ASC`,
        setList
      );
      remain = poolCards.length;
    } else {
      const params = [];
      let baseSql = `SELECT w.id
        FROM words w
        LEFT JOIN themes t ON t.id = w.theme_id
        LEFT JOIN theme_levels l ON l.id = w.level_id
        LEFT JOIN user_word_overrides uwo ON uwo.word_id = w.id AND uwo.user_id = ?
        WHERE w.active = 1
          AND COALESCE(uwo.active, 1) = 1
          AND (w.theme_id IS NULL OR t.active = 1)
          AND (w.level_id IS NULL OR l.active = 1)`;
      params.push(req.session.user.id);
      if (themeList.length === 1) {
        baseSql += ' AND w.theme_id = ?';
        params.push(themeList[0]);
      } else if (themeList.length > 1) {
        const ph = themeList.map(() => '?').join(',');
        baseSql += ` AND w.theme_id IN (${ph})`;
        params.push(...themeList);
      }
      if (level_id) {
        baseSql += ' AND w.level_id = ?';
        params.push(level_id);
      }
      if (difficulty) {
        baseSql += ' AND w.difficulty = ?';
        params.push(difficulty);
      }
      baseSql += ' AND (w.user_id IS NULL OR w.user_id = ?)';
      params.push(req.session.user.id);
      baseSql += ' ORDER BY w.id ASC';
      poolWords = await all(baseSql, params);
      remain = poolWords.length;
    }
  }

  if (!remain || Number(remain) <= 0) {
    const userId = req.session.user.id;
    const themes = await all('SELECT * FROM themes WHERE (user_id IS NULL OR user_id = ?) AND active = 1 ORDER BY id ASC', [userId]);
    const levels = await getLevelsForUser(userId);
    const sets = await all('SELECT id, name FROM sets WHERE user_id = ? AND active = 1 ORDER BY created_at DESC', [userId]);
    return res.render('train_setup', {
      themes,
      levels,
      sets,
      error: 'Aucun mot disponible avec cette selection.',
      params: {
        modes: modeList,
      rev_mode: rev_mode || 'order',
        difficulty: difficulty || '',
        theme_id: '',
        level_id: level_id || '',
        scope: 'all',
        remaining: 10,
        total: 10
      }
    });
  }

  req.session.trainState = {
    modes: modeList,
    theme_ids: themeList,
    set_ids: setList,
    level_id: level_id || '',
    difficulty: difficulty || '',
    rev_mode: rev_mode || 'order',
    scope: 'all',
    remaining: remain,
    total: remain,
    answered: 0,
    correct: 0,
    usedWordIds: [],
    usedCardIds: [],
    poolWords: poolWords ? poolWords.map(w => w.id) : null,
    poolCards: poolCards ? poolCards.map(c => c.id) : null
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
  const mode = modeList[0] || MODE_FLASHCARDS;
  const questionMode = pickQuestionMode(modeList);
  state.currentMode = questionMode;

  const remainingCount = Number(state.remaining || 0);
  const totalCount = Number(state.total || 0);
  const answeredCount = Number(state.answered || 0);
  const correctCount = Number(state.correct || 0);
  const themeList = state.theme_ids || [];
  const selectedSetIds = state.set_ids || [];

  const filters = {
    theme_id: themeList.length === 1 ? themeList[0] : null,
    theme_ids: themeList,
    level_id: state.level_id || null,
    difficulty: state.difficulty || '',
    rev_mode: state.rev_mode || 'random',
    scope: 'all',
    scope_list: [],
    source: selectedSetIds.length > 0 ? 'cards' : 'words',
    set_ids: selectedSetIds
  };

  const themes = await all('SELECT * FROM themes WHERE (user_id IS NULL OR user_id = ?) AND active = 1 ORDER BY id ASC', [userId]);

  if (remainingCount <= 0) {
    delete req.session.trainState;
  } else {
    req.session.trainState = state;
  }

  if (remainingCount <= 0) {
    return res.render('train', {
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
    if (filters.source === 'cards') {
      const placeholders = filters.set_ids.map(() => '?').join(',');
      const cardsPool = await all(
        `SELECT * FROM cards WHERE set_id IN (${placeholders}) AND active = 1`,
        filters.set_ids
      );
      if (!cardsPool || cardsPool.length === 0) {
        req.session.trainState = state;
        return res.render('train', {
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

      let availableCards = cardsPool;
      if (state.usedCardIds.length > 0) {
        const usedSet = new Set(state.usedCardIds.map(id => Number(id)).filter(id => !Number.isNaN(id)));
        availableCards = cardsPool.filter(c => !usedSet.has(Number(c.id)));
      }
      if (availableCards.length === 0) {
        state.usedCardIds = [];
        availableCards = cardsPool;
      }

      const pick = availableCards[Math.floor(Math.random() * availableCards.length)];
      const cardId = Number(pick.id);
      const cardKey = `card_${cardId}`;
      const word = { id: cardKey, hebrew: pick.hebrew, transliteration: pick.transliteration, french: pick.french };
      if (!state.usedCardIds.includes(cardId)) {
        state.usedCardIds.push(cardId);
      }
      req.session.trainState = state;

      if (questionMode === MODE_WRITTEN) {
        return res.render('train', {
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
      return res.render('train', {
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

    const excludedIds = state.usedWordIds.map(id => Number(id)).filter(id => !Number.isNaN(id));
    const word = await fetchNextWord(userId, { ...filters, excludedIds });
    if (word) {
      const wordId = Number(word.id);
      if (!state.usedWordIds.includes(wordId)) {
        state.usedWordIds.push(wordId);
      }
    }
    req.session.trainState = state;

    const isReverse = questionMode === MODE_FLASHCARDS_REVERSE;
    const options =
      word && (questionMode === MODE_FLASHCARDS || questionMode === MODE_FLASHCARDS_REVERSE)
        ? await getFlashcardOptions(userId, word, filters, isReverse)
        : null;
    const message = !word ? 'Aucun mot pour ces criteres.' : null;
    res.render('train', {
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
    res.render('train', {
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
  const filters = {
    theme_id: state.theme_id || null,
    theme_ids: state.theme_ids || [],
    level_id: state.level_id || null,
    difficulty: state.difficulty || '',
    rev_mode: state.rev_mode || 'random',
    scope: state.scope || 'all',
    scope_list: [],
    source: state.set_ids && state.set_ids.length > 0 ? 'cards' : 'words',
    set_ids: state.set_ids || []
  };

  const remainingCount = Math.max(0, Number(state.remaining || 1) - 1);
  const totalCount = Number(state.total || state.remaining || 1);
  const answeredCount = Number(state.answered || (totalCount - remainingCount - 1)) + 1;
  let correctCount = Number(state.correct || 0);
  const normalizedAnswer = (user_answer || '').trim().toLowerCase();

  try {
    const themes = await all('SELECT * FROM themes WHERE (user_id IS NULL OR user_id = ?) AND active = 1 ORDER BY id ASC', [userId]);

    if (filters.source === 'cards' && String(word_id).startsWith('card_')) {
      const cardId = Number(String(word_id).replace('card_', ''));
      const card = await get('SELECT * FROM cards WHERE id = ?', [cardId]);
      if (!card) return res.redirect('/train');
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

      return res.render('train', {
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

    const isCorrect = normalizedAnswer && normalizedAnswer === (word.french || '').trim().toLowerCase();
    if (isCorrect) correctCount += 1;

    const progressUpdate = await upsertProgress(userId, word_id, isCorrect);
    if (remainingCount > 0) {
      req.session.trainState = { ...state, correct: correctCount, answered: answeredCount, remaining: remainingCount, total: totalCount, modes: modeList };
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

    return res.render('train', {
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

app.get('/admin/users', requireAdmin, async (req, res) => {
  try {
    const users = await all(
      'SELECT id, email, display_name, role, created_at FROM users ORDER BY id ASC'
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
  const { email, password, display_name, role } = req.body;
  if (!email || !password || !display_name || !role) {
    return res.redirect('/admin/users');
  }
  try {
    const hash = await bcrypt.hash(password, 10);
    await run(
      'INSERT INTO users (email, password_hash, display_name, role) VALUES (?,?,?,?)',
      [email, hash, display_name, role]
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
  const { email, password, display_name, role } = req.body;
  try {
    if (password) {
      const hash = await bcrypt.hash(password, 10);
      await run(
        `UPDATE users SET email = ?, display_name = ?, role = ?, password_hash = ? WHERE id = ?`,
        [email, display_name, role, hash, req.params.id]
      );
    } else {
      await run(
        `UPDATE users SET email = ?, display_name = ?, role = ? WHERE id = ?`,
        [email, display_name, role, req.params.id]
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
    await run('DELETE FROM progress WHERE user_id = ?', [req.params.id]);
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
    const importError = req.query.import_error || null;
    const importSuccess = req.query.import_success || null;
    const themes = await all(
      `SELECT t.*,
        COUNT(w.id) AS word_count
       FROM themes t
       LEFT JOIN words w ON w.theme_id = t.id AND w.user_id IS NULL
       WHERE t.user_id IS NULL
       GROUP BY t.id
       ORDER BY ${orderBy}`
    );
    res.render('admin/themes', { themes, sort, importError, importSuccess });
  } catch (e) {
    console.error(e);
    res.render('admin/themes', { themes: [], sort: req.query.sort || 'created_asc', importError: null, importSuccess: null });
  }
});

// Import d'un thème par défaut (JSON)
app.post('/admin/themes/import', requireAdmin, async (req, res) => {
  const redirectErr = msg => res.redirect(`/admin/themes?import_error=${encodeURIComponent(msg)}`);
  try {
    const payload = req.body.payload;
    if (!payload || !payload.trim()) return redirectErr('Fichier JSON manquant');
    let data;
    try {
      data = JSON.parse(payload);
    } catch (e) {
      return redirectErr('JSON invalide');
    }
    const name = data && data.name && String(data.name).trim();
    if (!name) return redirectErr('Nom de theme manquant');
    const words = Array.isArray(data.words) ? data.words : [];
    if (words.length === 0) return redirectErr('Aucun mot dans le fichier');

    const active = data.active === 0 ? 0 : 1;
    const themeRes = await run('INSERT INTO themes (name, active, user_id, created_at) VALUES (?,?,NULL, CURRENT_TIMESTAMP)', [name, active]);
    const themeId = themeRes.lastID;

    for (const w of words) {
      const hebrew = w.hebrew ? String(w.hebrew).trim() : '';
      const french = w.french ? String(w.french).trim() : '';
      if (!hebrew || !french) continue;
      const translit = w.transliteration ? String(w.transliteration).trim() : null;
      const wActive = w.active === 0 ? 0 : 1;
      await run(
        `INSERT INTO words (hebrew, transliteration, french, theme_id, level_id, difficulty, active, user_id)
         VALUES (?,?,?,?,?,?,?,NULL)`,
        [hebrew, translit, french, themeId, null, 1, wActive]
      );
    }

    return res.redirect(`/admin/themes?import_success=${encodeURIComponent('Theme importe avec succes')}`);
  } catch (e) {
    console.error('Import theme error:', e);
    return res.redirect(`/admin/themes?import_error=${encodeURIComponent('Erreur pendant l import')}`);
  }
});

app.get('/admin/themes/new', requireAdmin, async (req, res) => {
  try {
    res.render('admin/theme_form', {
      theme: null,
      cards: [{ hebrew: '', french: '', transliteration: '' }],
      action: '/admin/themes'
    });
  } catch (e) {
    console.error(e);
    res.redirect('/admin/themes');
  }
});

app.post('/admin/themes', requireAdmin, async (req, res) => {
  const { name, active } = req.body;
  const cards = normalizeCardsPayload(req.body.cards);
  try {
    const createdTheme = await run(
      'INSERT INTO themes (name, active, user_id, created_at) VALUES (?,?,NULL, CURRENT_TIMESTAMP)',
      [name, active ? 1 : 0]
    );
    const newThemeId = createdTheme.lastID;
    if (newThemeId && cards.length > 0) {
      for (const card of cards) {
        await run(
          `INSERT INTO words (hebrew, transliteration, french, theme_id, level_id, difficulty, active, user_id)
           VALUES (?,?,?,?,?,?,?,NULL)`,
          [card.hebrew, card.transliteration || null, card.french, newThemeId, null, 1, 1]
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
      'SELECT id, hebrew, french, transliteration FROM words WHERE theme_id = ? AND user_id IS NULL ORDER BY id ASC',
      [theme.id]
    );
    res.render('admin/theme_form', {
      theme,
      cards: cards.length > 0 ? cards : [{ hebrew: '', french: '', transliteration: '' }],
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
      'SELECT id, hebrew, french, transliteration, active FROM words WHERE theme_id = ? AND user_id IS NULL ORDER BY id ASC',
      [theme.id]
    );
    res.render('admin/theme_show', { theme, words });
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
  const { name, active } = req.body;
  const cards = normalizeCardsPayload(req.body.cards);
  try {
    const theme = await get('SELECT * FROM themes WHERE id = ? AND user_id IS NULL', [req.params.id]);
    if (!theme) return res.redirect('/admin/themes');

    await run(
      'UPDATE themes SET name = ?, active = ? WHERE id = ?',
      [name, active ? 1 : 0, theme.id]
    );

    const existing = await all('SELECT * FROM words WHERE theme_id = ? AND user_id IS NULL', [theme.id]);
    const existingById = new Map(existing.map(w => [Number(w.id), w]));
    const keptIds = new Set();

    for (const card of cards) {
      if (card.id && existingById.has(Number(card.id))) {
        const current = existingById.get(Number(card.id));
        await run(
          'UPDATE words SET hebrew = ?, french = ?, transliteration = ? WHERE id = ? AND theme_id = ? AND user_id IS NULL',
          [card.hebrew, card.french, card.transliteration || null, current.id, theme.id]
        );
        keptIds.add(Number(card.id));
      } else {
        const created = await run(
          `INSERT INTO words (hebrew, transliteration, french, theme_id, level_id, difficulty, active, user_id)
           VALUES (?,?,?,?,?,?,?,NULL)`,
          [card.hebrew, card.transliteration || null, card.french, theme.id, null, 1, 1]
        );
        keptIds.add(created.lastID);
      }
    }

    for (const w of existing) {
      if (!keptIds.has(Number(w.id))) {
        await run('DELETE FROM progress WHERE word_id = ?', [w.id]);
        await run('DELETE FROM favorites WHERE word_id = ?', [w.id]);
        await run('DELETE FROM words WHERE id = ?', [w.id]);
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
    await run('DELETE FROM progress WHERE word_id IN (SELECT id FROM words WHERE theme_id = ? AND user_id IS NULL)', [req.params.id]);
    await run('DELETE FROM favorites WHERE word_id IN (SELECT id FROM words WHERE theme_id = ? AND user_id IS NULL)', [req.params.id]);
    await run('DELETE FROM words WHERE theme_id = ? AND user_id IS NULL', [req.params.id]);
    await run('DELETE FROM theme_levels WHERE theme_id = ?', [req.params.id]);
    await run('DELETE FROM themes WHERE id = ? AND user_id IS NULL', [req.params.id]);
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
    await run(
      `INSERT INTO words (hebrew, transliteration, french, theme_id, level_id, difficulty, active)
       VALUES (?,?,?,?,?,?,?)`,
      [hebrew, transliteration || null, french, themeId || null, levelId, difficulty || 1, active ? 1 : 0]
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







































































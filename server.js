/**
 * Hebrew Duo Pro - Express + SQLite
 */
const express = require('express');
const session = require('express-session');
const SQLiteStore = require('connect-sqlite3')(session);
const bcrypt = require('bcrypt');
const methodOverride = require('method-override');
const path = require('path');
const expressLayouts = require('express-ejs-layouts');

const db = require('./db');

// Helpers promisifiés pour sqlite3
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

async function fetchNextWord(userId, filters = {}) {
  const mode = (filters.rev_mode || 'weak').toLowerCase();
  const params = [userId, userId];
  let sql = `SELECT w.*,
        IFNULL(p.strength, 0) AS strength,
        p.last_seen,
        p.id AS progress_id,
        fav.id AS fav_id
       FROM words w
       LEFT JOIN progress p ON p.word_id = w.id AND p.user_id = ?
       LEFT JOIN favorites fav ON fav.word_id = w.id AND fav.user_id = ?
       WHERE w.active = 1`;

  if (filters.theme_id) {
    sql += ' AND w.theme_id = ?';
    params.push(filters.theme_id);
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

  if (mode === 'favorites' || mode === 'favorite') {
    sql += ' AND fav.id IS NOT NULL';
  } else if (mode === 'new') {
    sql += ' AND p.id IS NULL';
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

async function getFlashcardOptions(userId, currentWord, filters = {}) {
  const params = [userId, currentWord.id];
  let sql = `SELECT w.id, w.french
    FROM words w
    LEFT JOIN progress p ON p.word_id = w.id AND p.user_id = ?
    WHERE w.active = 1 AND w.id != ?`;

  if (filters.theme_id) {
    sql += ' AND w.theme_id = ?';
    params.push(filters.theme_id);
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
  const options = [...rows.map(r => ({ id: r.id, french: r.french })), { id: currentWord.id, french: currentWord.french }];
  // shuffle
  for (let i = options.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [options[i], options[j]] = [options[j], options[i]];
  }
  return options;
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
app.use(express.urlencoded({ extended: true }));
app.use(methodOverride('_method'));

app.use(
  session({
    store: new SQLiteStore({ db: 'sessions.db', dir: '.' }),
    secret: 'change-moi-en-secret-solide',
    resave: false,
    saveUninitialized: false,
    cookie: { maxAge: 1000 * 60 * 60 * 24 * 7 }
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
    return res.status(403).send('Accès refusé');
  }
  next();
}

// Admin par défaut
async function ensureAdmin() {
  const admin = await get("SELECT * FROM users WHERE role = 'admin'");
  if (!admin) {
    const hash = await bcrypt.hash('admin123', 10);
    await run(
      'INSERT INTO users (email, password_hash, display_name, role) VALUES (?,?,?,?)',
      ['admin@example.com', hash, 'Admin', 'admin']
    );
    console.log('Admin créé : admin@example.com / admin123');
  }
}

// Thèmes de base
async function ensureBaseThemes() {
  const row = await get('SELECT COUNT(*) AS c FROM themes', []);
  const count = row ? row.c : 0;
  if (count === 0) {
    const base = await run('INSERT INTO themes (name, parent_id) VALUES (?,?)', ['Base', null]);
    const baseId = base.lastID;
    await run('INSERT INTO themes (name, parent_id) VALUES (?,?)', ['Salutations', baseId]);
    await run('INSERT INTO themes (name, parent_id) VALUES (?,?)', ['Nourriture', baseId]);
    await run('INSERT INTO themes (name, parent_id) VALUES (?,?)', ['Voyage', baseId]);
    console.log('Thèmes de base créés.');
  }
}

(async () => {
  try {
    await ensureAdmin();
    await ensureBaseThemes();
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
      return res.render('register', { error: 'Cet email est déjà utilisé.' });
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

// ---------- Dashboard utilisateur ----------
app.get('/app', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  try {
    const stats = await get(
      `SELECT
        COUNT(DISTINCT word_id) AS words_seen,
        SUM(success_count) AS total_success,
        SUM(fail_count) AS total_fail,
        AVG(strength) AS avg_strength
       FROM progress WHERE user_id = ?`,
      [userId]
    );

    const themes = await all(
      `SELECT t.*,
        (SELECT COUNT(*) FROM words w WHERE w.theme_id = t.id) AS word_count
       FROM themes t
       WHERE t.parent_id IS NULL
       ORDER BY t.name`
    );

    res.render('app', { stats, themes });
  } catch (e) {
    console.error(e);
    res.render('app', { stats: null, themes: [] });
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

// ---------- Bibliothèque perso / globale ----------
app.get('/my/words', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  try {
    const globalWords = await all(
      `SELECT w.*, t.name AS theme_name
       FROM words w
       LEFT JOIN themes t ON t.id = w.theme_id
       WHERE w.user_id IS NULL AND w.active = 1
       ORDER BY w.created_at DESC`
    );
    const personalWords = await all(
      `SELECT w.*, t.name AS theme_name
       FROM words w
       LEFT JOIN themes t ON t.id = w.theme_id
       WHERE w.user_id = ?
       ORDER BY w.created_at DESC`,
      [userId]
    );
    const themes = await all('SELECT * FROM themes ORDER BY name');
    res.render('my_words', { globalWords, personalWords, themes });
  } catch (e) {
    console.error(e);
    res.render('my_words', { globalWords: [], personalWords: [], themes: [] });
  }
});

app.get('/my/words/new', requireAuth, async (req, res) => {
  try {
    const themes = await all('SELECT * FROM themes ORDER BY name');
    res.render('my_word_form', {
      word: null,
      themes,
      action: '/my/words'
    });
  } catch (e) {
    console.error(e);
    res.redirect('/my/words');
  }
});

app.post('/my/words', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const { hebrew, transliteration, french, theme_id, difficulty, active } = req.body;
  try {
    await run(
      `INSERT INTO words (hebrew, transliteration, french, theme_id, difficulty, active, user_id)
       VALUES (?,?,?,?,?,?,?)`,
      [hebrew, transliteration || null, french, theme_id || null, difficulty || 1, active ? 1 : 0, userId]
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
    const themes = await all('SELECT * FROM themes ORDER BY name');
    res.render('my_word_form', {
      word,
      themes,
      action: `/my/words/${word.id}?_method=PUT`
    });
  } catch (e) {
    console.error(e);
    res.redirect('/my/words');
  }
});

app.put('/my/words/:id', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const { hebrew, transliteration, french, theme_id, difficulty, active } = req.body;
  try {
    await run(
      `UPDATE words
       SET hebrew = ?, transliteration = ?, french = ?, theme_id = ?, difficulty = ?, active = ?
       WHERE id = ? AND user_id = ?`,
      [hebrew, transliteration || null, french, theme_id || null, difficulty || 1, active ? 1 : 0, req.params.id, userId]
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

// ---------- Thèmes perso ----------
app.get('/my/themes', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  try {
    const themes = await all(
      `SELECT t.*, p.name AS parent_name
       FROM themes t
       LEFT JOIN themes p ON p.id = t.parent_id
       WHERE t.user_id = ?
       ORDER BY t.name`,
      [userId]
    );
    res.render('my_themes', { themes });
  } catch (e) {
    console.error(e);
    res.render('my_themes', { themes: [] });
  }
});

app.get('/my/themes/new', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const parents = await all('SELECT * FROM themes WHERE user_id = ? ORDER BY name', [userId]);
  res.render('my_theme_form', { theme: null, parents, action: '/my/themes' });
});

app.post('/my/themes', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const { name, parent_id } = req.body;
  if (!name) return res.redirect('/my/themes');
  try {
    await run('INSERT INTO themes (name, parent_id, user_id) VALUES (?,?,?)', [name, parent_id || null, userId]);
    res.redirect('/my/themes');
  } catch (e) {
    console.error(e);
    res.redirect('/my/themes');
  }
});

app.get('/my/themes/:id/edit', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  try {
    const theme = await get('SELECT * FROM themes WHERE id = ? AND user_id = ?', [req.params.id, userId]);
    if (!theme) return res.redirect('/my/themes');
    const parents = await all('SELECT * FROM themes WHERE user_id = ? AND id != ? ORDER BY name', [userId, req.params.id]);
    res.render('my_theme_form', { theme, parents, action: `/my/themes/${theme.id}?_method=PUT` });
  } catch (e) {
    console.error(e);
    res.redirect('/my/themes');
  }
});

app.put('/my/themes/:id', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const { name, parent_id } = req.body;
  try {
    await run('UPDATE themes SET name = ?, parent_id = ? WHERE id = ? AND user_id = ?', [name, parent_id || null, req.params.id, userId]);
    res.redirect('/my/themes');
  } catch (e) {
    console.error(e);
    res.redirect('/my/themes');
  }
});

app.delete('/my/themes/:id', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  try {
    await run('UPDATE words SET theme_id = NULL WHERE theme_id = ? AND user_id = ?', [req.params.id, userId]);
    await run('UPDATE themes SET parent_id = NULL WHERE parent_id = ? AND user_id = ?', [req.params.id, userId]);
    await run('DELETE FROM themes WHERE id = ? AND user_id = ?', [req.params.id, userId]);
    res.redirect('/my/themes');
  } catch (e) {
    console.error(e);
    res.redirect('/my/themes');
  }
});

// ---------- Entraînement ----------
app.get('/train/setup', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const themes = await all('SELECT * FROM themes WHERE user_id IS NULL OR user_id = ? ORDER BY name', [userId]);
  res.render('train_setup', {
    themes,
    params: {
      mode: 'flashcards',
      rev_mode: 'weak',
      difficulty: '',
      theme_id: '',
      scope: 'all',
      remaining: 10,
      total: 10
    }
  });
});

app.get('/train', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const { theme_id, difficulty, rev_mode, scope, remaining, total, answered, correct, mode: queryMode } = req.query;
  const mode = queryMode === 'quiz' ? 'quiz' : 'flashcards';
  const filters = { theme_id, difficulty, rev_mode: rev_mode || 'weak', scope: scope || 'all' };
  const remainingCount = Number(remaining || 10);
  const totalCount = Number(total || remainingCount);
  const answeredCount = Number(answered || (totalCount - remainingCount));
  const correctCount = Number(correct || 0);

  if (remainingCount <= 0) {
    return res.render('train', {
      word: null,
      message: 'Session terminée.',
      result: null,
      mode,
      filters,
      options: null,
      themes: await all('SELECT * FROM themes WHERE user_id IS NULL OR user_id = ? ORDER BY name', [userId]),
      remaining: 0,
      total: totalCount,
      answered: answeredCount,
      correct: correctCount,
      nextUrl: null
    });
  }

  try {
    const themes = await all('SELECT * FROM themes WHERE user_id IS NULL OR user_id = ? ORDER BY name', [userId]);
    const word = await fetchNextWord(userId, filters);
    const options = word && mode === 'flashcards' ? await getFlashcardOptions(userId, word, filters) : null;
    const message = !word ? 'Aucun mot pour ces critères.' : null;
    res.render('train', {
      word,
      message,
      result: null,
      mode,
      filters,
      options,
      themes,
      remaining: remainingCount,
      total: totalCount,
      answered: answeredCount,
      correct: correctCount,
      nextUrl: null
    });
  } catch (e) {
    console.error(e);
    res.render('train', {
      word: null,
      message: 'Erreur serveur.',
      result: null,
      mode,
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
  const { word_id, user_answer, theme_id, difficulty, rev_mode, scope, remaining, total, answered, correct } = req.body;
  const filters = { theme_id, difficulty, rev_mode: rev_mode || 'weak', scope: scope || 'all' };
  const remainingCount = Math.max(0, Number(remaining || 1) - 1);
  const totalCount = Number(total || remainingCount + 1);
  const answeredCount = Number(answered || (totalCount - remainingCount - 1)) + 1;
  let correctCount = Number(correct || 0);

  try {
    const word = await get('SELECT * FROM words WHERE id = ?', [word_id]);
    if (!word) return res.redirect('/train');

    const isCorrect = user_answer.trim().toLowerCase() === word.hebrew.trim().toLowerCase();
    if (isCorrect) correctCount += 1;

    const progressUpdate = await upsertProgress(userId, word_id, isCorrect);

    const themes = await all('SELECT * FROM themes WHERE user_id IS NULL OR user_id = ? ORDER BY name', [userId]);

    const result = {
      isCorrect,
      correctAnswer: word.hebrew,
      transliteration: word.transliteration,
      french: word.french,
      newStrength: progressUpdate ? progressUpdate.strength : null
    };

    const nextUrl =
      remainingCount > 0
        ? `/train?mode=quiz&theme_id=${filters.theme_id || ''}&difficulty=${filters.difficulty || ''}&rev_mode=${filters.rev_mode || 'weak'}&scope=${filters.scope || 'all'}&remaining=${remainingCount}&total=${totalCount}&answered=${answeredCount}&correct=${correctCount}`
        : null;

    res.render('train', {
      word: null,
      message: remainingCount > 0 ? null : 'Session terminée.',
      result,
      mode: 'quiz',
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
    res.redirect('/train');
  }
});

app.post('/train/flashcards', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const { word_id, choice_word_id, theme_id, difficulty, rev_mode, scope, remaining, total, answered, correct } = req.body;
  const filters = { theme_id, difficulty, rev_mode: rev_mode || 'weak', scope: scope || 'all' };
  const remainingCount = Math.max(0, Number(remaining || 1) - 1);
  const totalCount = Number(total || remainingCount + 1);
  const answeredCount = Number(answered || (totalCount - remainingCount - 1)) + 1;
  let correctCount = Number(correct || 0);

  try {
    const word = await get('SELECT * FROM words WHERE id = ?', [word_id]);
    if (!word) return res.redirect('/train?mode=flashcards');
    const isCorrect = String(choice_word_id) === String(word_id);
    if (isCorrect) correctCount += 1;
    await upsertProgress(userId, word_id, isCorrect);

    const result = {
      isCorrect,
      correctAnswer: word.hebrew,
      transliteration: word.transliteration,
      french: word.french
    };

    const nextUrl =
      remainingCount > 0
        ? `/train?mode=flashcards&theme_id=${filters.theme_id || ''}&difficulty=${filters.difficulty || ''}&rev_mode=${filters.rev_mode || 'weak'}&scope=${filters.scope || 'all'}&remaining=${remainingCount}&total=${totalCount}&answered=${answeredCount}&correct=${correctCount}`
        : null;

    res.render('train', {
      word: null,
      message: remainingCount > 0 ? null : 'Session terminée.',
      result,
      mode: 'flashcards',
      filters,
      options: null,
      themes: await all('SELECT * FROM themes WHERE user_id IS NULL OR user_id = ? ORDER BY name', [userId]),
      remaining: remainingCount,
      total: totalCount,
      answered: answeredCount,
      correct: correctCount,
      nextUrl
    });
  } catch (e) {
    console.error(e);
    res.redirect('/train?mode=flashcards');
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
      'SELECT id, email, display_name, role, created_at FROM users ORDER BY created_at DESC'
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
    res.redirect('/admin/users');
  } catch (e) {
    console.error(e);
    res.redirect('/admin/users');
  }
});

app.get('/admin/themes', requireAdmin, async (req, res) => {
  try {
    const themes = await all(
      `SELECT t.*, parent.name AS parent_name
       FROM themes t
       LEFT JOIN themes parent ON parent.id = t.parent_id
       ORDER BY t.name`
    );
    res.render('admin/themes', { themes });
  } catch (e) {
    console.error(e);
    res.render('admin/themes', { themes: [] });
  }
});

app.get('/admin/themes/new', requireAdmin, async (req, res) => {
  try {
    const parents = await all('SELECT * FROM themes ORDER BY name');
    res.render('admin/theme_form', {
      theme: null,
      parents,
      action: '/admin/themes'
    });
  } catch (e) {
    console.error(e);
    res.redirect('/admin/themes');
  }
});

app.post('/admin/themes', requireAdmin, async (req, res) => {
  const { name, parent_id } = req.body;
  try {
    await run(
      'INSERT INTO themes (name, parent_id) VALUES (?,?)',
      [name, parent_id || null]
    );
    res.redirect('/admin/themes');
  } catch (e) {
    console.error(e);
    res.redirect('/admin/themes');
  }
});

app.get('/admin/themes/:id/edit', requireAdmin, async (req, res) => {
  try {
    const theme = await get('SELECT * FROM themes WHERE id = ?', [req.params.id]);
    if (!theme) return res.redirect('/admin/themes');
    const parents = await all('SELECT * FROM themes WHERE id != ? ORDER BY name', [req.params.id]);
    res.render('admin/theme_form', {
      theme,
      parents,
      action: `/admin/themes/${theme.id}?_method=PUT`
    });
  } catch (e) {
    console.error(e);
    res.redirect('/admin/themes');
  }
});

app.put('/admin/themes/:id', requireAdmin, async (req, res) => {
  const { name, parent_id } = req.body;
  try {
    await run(
      'UPDATE themes SET name = ?, parent_id = ? WHERE id = ?',
      [name, parent_id || null, req.params.id]
    );
    res.redirect('/admin/themes');
  } catch (e) {
    console.error(e);
    res.redirect('/admin/themes');
  }
});

app.delete('/admin/themes/:id', requireAdmin, async (req, res) => {
  try {
    await run('UPDATE words SET theme_id = NULL WHERE theme_id = ?', [req.params.id]);
    await run('UPDATE themes SET parent_id = NULL WHERE parent_id = ?', [req.params.id]);
    await run('DELETE FROM themes WHERE id = ?', [req.params.id]);
    res.redirect('/admin/themes');
  } catch (e) {
    console.error(e);
    res.redirect('/admin/themes');
  }
});

app.get('/admin/words', requireAdmin, async (req, res) => {
  try {
    const words = await all(
      `SELECT w.*, t.name AS theme_name
       FROM words w
       LEFT JOIN themes t ON t.id = w.theme_id
       WHERE w.user_id IS NULL
       ORDER BY w.created_at DESC`
    );
    res.render('admin/words', { words });
  } catch (e) {
    console.error(e);
    res.render('admin/words', { words: [] });
  }
});

app.get('/admin/words/new', requireAdmin, async (req, res) => {
  try {
    const themes = await all('SELECT * FROM themes ORDER BY name');
    res.render('admin/word_form', {
      word: null,
      themes,
      action: '/admin/words'
    });
  } catch (e) {
    console.error(e);
    res.redirect('/admin/words');
  }
});

app.post('/admin/words', requireAdmin, async (req, res) => {
  const { hebrew, transliteration, french, theme_id, difficulty, active } = req.body;
  try {
    await run(
      `INSERT INTO words (hebrew, transliteration, french, theme_id, difficulty, active)
       VALUES (?,?,?,?,?,?)`,
      [hebrew, transliteration || null, french, theme_id || null, difficulty || 1, active ? 1 : 0]
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
    const themes = await all('SELECT * FROM themes ORDER BY name');
    res.render('admin/word_form', {
      word,
      themes,
      action: `/admin/words/${word.id}?_method=PUT`
    });
  } catch (e) {
    console.error(e);
    res.redirect('/admin/words');
  }
});

app.put('/admin/words/:id', requireAdmin, async (req, res) => {
  const { hebrew, transliteration, french, theme_id, difficulty, active } = req.body;
  try {
    await run(
      `UPDATE words
       SET hebrew = ?, transliteration = ?, french = ?, theme_id = ?, difficulty = ?, active = ?
       WHERE id = ?`,
      [hebrew, transliteration || null, french, theme_id || null, difficulty || 1, active ? 1 : 0, req.params.id]
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
  console.log(`Serveur démarré sur http://localhost:${PORT}`);
});

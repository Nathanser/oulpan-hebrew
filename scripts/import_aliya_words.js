// UTF-8 script to reset and import Immigration et Alya words with proper accents/niqqudot
const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database('hebrew-duo.db');

const data = [
  { theme: "Immigration et Alya", level: "Niveau 1 - Les bases et l'Identité", french: "\"Monter\" en Israël (faire son Alya)", hebrew: "לַעֲלוֹת", transliteration: "la'alote", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 1 - Les bases et l'Identité", french: "Immigrer", hebrew: "לְהַגֵּר ל-", transliteration: "léhaguère le", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 1 - Les bases et l'Identité", french: "Citoyen", hebrew: "אֶזְרָח", transliteration: "ezrah'", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 1 - Les bases et l'Identité", french: "Étranger", hebrew: "תּוֹשָׁב זָר", transliteration: "toshav zare", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 1 - Les bases et l'Identité", french: "Nouveau pays", hebrew: "אֶרֶץ חֲדָשָׁה", transliteration: "eretz h'adashah", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 1 - Les bases et l'Identité", french: "Pays d'origine", hebrew: "אֶרֶץ מוֹצָא", transliteration: "eretz motsa", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 1 - Les bases et l'Identité", french: "Pays de naissance", hebrew: "אֶרֶץ לֵדָה", transliteration: "eretz leyda", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 1 - Les bases et l'Identité", french: "Carte d'identité", hebrew: "תְּעוּדַת זֶהוּת", transliteration: "té'oudate zehoute", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 1 - Les bases et l'Identité", french: "Passeport", hebrew: "דַּרְכּוֹן", transliteration: "darkone", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 1 - Les bases et l'Identité", french: "Visa / Permis d'entrée", hebrew: "אֲשֶׁרַת כְּנִיסָה, וִיזָה", transliteration: "ashrate knissa, viza", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 2 - Lieux et Installation", french: "S'installer à", hebrew: "לְהִתְיַשֵּׁב ב-", transliteration: "lehiteyashève be", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 2 - Lieux et Installation", french: "S'intégrer à", hebrew: "לְהִקָּלֵט ב-", transliteration: "lehikalète be", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 2 - Lieux et Installation", french: "S'inscrire à", hebrew: "לְהֵרָשֵׁם ל-", transliteration: "lehèrashème le", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 2 - Lieux et Installation", french: "Oulpan", hebrew: "אוּלְפָּן", transliteration: "oulpane", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 2 - Lieux et Installation", french: "Centre d'intégration", hebrew: "מֶרְכַּז קְלִיטָה", transliteration: "merkaze klitah", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 2 - Lieux et Installation", french: "Ministère de l'intégration", hebrew: "מִשְׂרַד הַקְּלִיטָה", transliteration: "missrade haklitah", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 2 - Lieux et Installation", french: "Ministère de l'immigration", hebrew: "מִשְׂרַד הַהֲגִירָה", transliteration: "missrade hahaguirah", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 2 - Lieux et Installation", french: "Frontières", hebrew: "גְּבוּלוֹת", transliteration: "gvoulote", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 2 - Lieux et Installation", french: "Bateau d'immigrants", hebrew: "אֳנִיּוֹת מְהַגְּרִים", transliteration: "oniyate mehagrime", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 2 - Lieux et Installation", french: "Immigrants", hebrew: "מְהַגְּרִים", transliteration: "mehagrime", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 3 - Administratif de base", french: "Remplir un formulaire", hebrew: "לְמַלֵּא טֹפֶס", transliteration: "lemalé tofèsse", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 3 - Administratif de base", french: "Dossier", hebrew: "תִיק", transliteration: "tik", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 3 - Administratif de base", french: "Copie", hebrew: "הֶעְתֵּק", transliteration: "he'etèke", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 3 - Administratif de base", french: "Certificats", hebrew: "תְּעוּדוֹת", transliteration: "té'oudote", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 3 - Administratif de base", french: "Certificat de naissance", hebrew: "תְּעוּדַת לֵדָה", transliteration: "té'oudate leyda", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 3 - Administratif de base", french: "Certificat de mariage", hebrew: "תְּעוּדַת נִשּׂוּאִין", transliteration: "té'oudate nissouïne", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 3 - Administratif de base", french: "Situation familiale", hebrew: "מַצָּב מִשְׁפַּחְתִּי", transliteration: "matsave mishepah'ti", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 3 - Administratif de base", french: "Natif", hebrew: "יְלִיד הָאָרֶץ", transliteration: "yelide haaretz", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 3 - Administratif de base", french: "Nationalité", hebrew: "אֶזְרָחוּת", transliteration: "ezrah'oute", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 3 - Administratif de base", french: "Double nationalité", hebrew: "אֶזְרָחוּת כְּפוּלָה", transliteration: "ezrah'oute kfoulah", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 4 - Travail et Économie", french: "Travailleur étranger", hebrew: "עוֹבֵד זָר", transliteration: "ôvède zare", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 4 - Travail et Économie", french: "Autorisation de travail", hebrew: "אֲשֶׁרַת עֲבוֹדָה", transliteration: "ashrate âvoda", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 4 - Travail et Économie", french: "Autorisation de séjour", hebrew: "אֲשֶׁרַת שְׁהִיָּה", transliteration: "ashrate shehiyah", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 4 - Travail et Économie", french: "Salaire minimum", hebrew: "שְׂכַר מִינִימוּם", transliteration: "skhare minimoume", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 4 - Travail et Économie", french: "Sécurité sociale", hebrew: "בִּטּוּחַ לְאֻמִּי", transliteration: "bitouah' léoumi", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 4 - Travail et Économie", french: "Certificat de travail", hebrew: "תְּעוּדַת עֲבוֹדָה", transliteration: "té'oudate âvoda", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 4 - Travail et Économie", french: "Qualifié", hebrew: "מֻסְמָךְ", transliteration: "moussemakhe", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 4 - Travail et Économie", french: "Travail saisonnier", hebrew: "עֲבוֹדָה עוֹנָתִית", transliteration: "âvodah ônatite", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 4 - Travail et Économie", french: "Travail au noir", hebrew: "עֲבוֹדָה שְׁחֹרָה", transliteration: "âvodah sheh'orah", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 4 - Travail et Économie", french: "Recevoir de l'aide", hebrew: "לְקַבֵּל עֶזְרָה", transliteration: "lekabèle êzrah", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 5 - Droits et Allocations", french: "Allocations familiales", hebrew: "קִצְבָּאוֹת", transliteration: "kitsebaote", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 5 - Droits et Allocations", french: "Allocations de chômage", hebrew: "דְּמֵי אַבְטָלָה", transliteration: "dmey avetelah", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 5 - Droits et Allocations", french: "Allocation de naissance", hebrew: "קִצְבַּת לֵדָה", transliteration: "kitsebate leyda", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 5 - Droits et Allocations", french: "Droit de vote", hebrew: "זְכוּת הַצְבָּעָה", transliteration: "zkhoute hatsba'ah", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 5 - Droits et Allocations", french: "Légal", hebrew: "חֻקִּי", transliteration: "h'ouki", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 5 - Droits et Allocations", french: "Illégal", hebrew: "בִּלְתִּי חֻקִּי", transliteration: "bilti h'ouki", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 5 - Droits et Allocations", french: "Conditions de vie", hebrew: "תְּנָאֵי חַיִּים", transliteration: "tnaey h'ayim", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 5 - Droits et Allocations", french: "Regroupement familial", hebrew: "אִחוּד מִשְׁפָּחוֹת", transliteration: "ih'oude mishepah'ote", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 5 - Droits et Allocations", french: "Naturaliser", hebrew: "לְהִתְאַזְרֵחַ", transliteration: "lehiteazréah'", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 5 - Droits et Allocations", french: "Naturalisation", hebrew: "קַבָּלַת אֶזְרָחוּת", transliteration: "kabalate ezrah'oute", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 6 - Difficultés et Actions", french: "Expulser", hebrew: "לְגָרֵשׁ", transliteration: "leguarèshe", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 6 - Difficultés et Actions", french: "Expulsion", hebrew: "גֵּרוּשׁ", transliteration: "guèroushe", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 6 - Difficultés et Actions", french: "Fuir de", hebrew: "לִבְרֹחַ מ-", transliteration: "livroah' me", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 6 - Difficultés et Actions", french: "Demander à", hebrew: "לְבַקֵּשׁ מ-", transliteration: "levakèshe me", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 6 - Difficultés et Actions", french: "Séparer", hebrew: "לְהַפְרִיד בֵּין", transliteration: "léhafride beyne", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 6 - Difficultés et Actions", french: "Renforcer les contrôles", hebrew: "לְחַזֵּק אֶת הַבְּדִיקוֹת", transliteration: "leh'azèke ète habdikote", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 6 - Difficultés et Actions", french: "Entrer clandestinement", hebrew: "לְהִכָּנֵס בְּלִי אִשּׁוּר", transliteration: "lehikanesse bli ishoure", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 6 - Difficultés et Actions", french: "Immigrant illégal", hebrew: "מְהַגֵּר בִּלְתִּי חֻקִּי", transliteration: "mehaguère bileti h'ouki", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 6 - Difficultés et Actions", french: "Passeur", hebrew: "מַבְרִיחַ", transliteration: "mavriah'", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 6 - Difficultés et Actions", french: "Contrebande", hebrew: "הַבְרָחָה", transliteration: "havrah'a", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 7 - Souffrance et Crises", french: "Réfugié(s)", hebrew: "פָּלִיט, פְּלִיטִים", transliteration: "palite, plitime", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 7 - Souffrance et Crises", french: "Camp de réfugiés", hebrew: "מַחֲנֵה פְּלִיטִים", transliteration: "mah'ané plitime", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 7 - Souffrance et Crises", french: "Asile politique", hebrew: "מִקְלָט מְדִינִי", transliteration: "miklate medini", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 7 - Souffrance et Crises", french: "Guerre civile", hebrew: "מִלְחֶמֶת אֶזְרָחִית", transliteration: "mileh'ama ezrah'ite", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 7 - Souffrance et Crises", french: "Famine", hebrew: "רָעָב", transliteration: "ra'ave", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 7 - Souffrance et Crises", french: "Pauvreté", hebrew: "עֹנִי", transliteration: "ôni", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 7 - Souffrance et Crises", french: "Tortures", hebrew: "עִנּוּיִים", transliteration: "înouyime", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 7 - Souffrance et Crises", french: "Esclavage", hebrew: "עַבְדוּת", transliteration: "âvedoute", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 7 - Souffrance et Crises", french: "Prostitution", hebrew: "זְנוּת", transliteration: "znoute", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 7 - Souffrance et Crises", french: "Analphabète", hebrew: "אֲנָלְפָבִּית", transliteration: "analfabète", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 8 - Documents et Juridique", french: "Documents officiels", hebrew: "מִסְמָכִים רִשְׁמִיִּים", transliteration: "missemakhime rishemiime", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 8 - Documents et Juridique", french: "Faux documents", hebrew: "מִסְמָכִים מְזֻיָּפִים", transliteration: "missemakhim mezouyafim", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 8 - Documents et Juridique", french: "Mariages fictifs", hebrew: "נִשּׂוּאִים פִיקְטִיבִיִּים", transliteration: "nissouïme fiktiviime", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 8 - Documents et Juridique", french: "Prises de sang", hebrew: "בְּדִיקוֹת דָּם", transliteration: "bedikote dame", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 8 - Documents et Juridique", french: "Certificat de décès", hebrew: "תְּעוּדַת פְּטִירָה", transliteration: "té'oudate petira", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 8 - Documents et Juridique", french: "Être domicilié à", hebrew: "לְהִתְגּוֹרֵר ב-", transliteration: "lehiteguorère be", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 8 - Documents et Juridique", french: "Politique d'immigration", hebrew: "מְדִינִיּוּת הֲגִירָה", transliteration: "medinioute haguirah", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 8 - Documents et Juridique", french: "Police de l'immigration", hebrew: "מִשְׁטֶרֶת הַהֲגִירָה", transliteration: "mishetèrète hahaguirah", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 8 - Documents et Juridique", french: "Libre circulation", hebrew: "חֹפֶשׁ תְּנוּעָה", transliteration: "h'ofèshe tnoua'", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 8 - Documents et Juridique", french: "Déplacement de populations", hebrew: "הַעֲבָרַת אֻכְלוּסִיּוֹת", transliteration: "ha'avarate okhloussiote", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 9 - Société et Préjugés", french: "Discrimination", hebrew: "אַפְלָיָה", transliteration: "aflayah", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 9 - Société et Préjugés", french: "Discrimination raciale", hebrew: "אַפְלָיָה גִּזְעִית", transliteration: "aflayah guiz'ite", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 9 - Société et Préjugés", french: "Racisme", hebrew: "גִּזְעָנוּת", transliteration: "guiz'anoute", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 9 - Société et Préjugés", french: "Préjugés", hebrew: "דֵּעוֹת קְדוּמוֹת", transliteration: "dé'ote kdoumote", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 9 - Société et Préjugés", french: "Minorité ethnique", hebrew: "מִעוּט עֲדָתִי", transliteration: "mi'oute adati", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 9 - Société et Préjugés", french: "Communautaire", hebrew: "עֲדָתִי", transliteration: "âdati", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 9 - Société et Préjugés", french: "Relations inter raciales", hebrew: "יְחָסִים בֵּין גְּזָעִים", transliteration: "yah'assime beyne guizime", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 9 - Société et Préjugés", french: "Exploité", hebrew: "מְנֻצָּל", transliteration: "menoutsale", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 9 - Société et Préjugés", french: "Difficultés de language", hebrew: "קְשָׁיֵי שָׂפָה", transliteration: "kshayé safah", active: 1 },
  { theme: "Immigration et Alya", level: "Niveau 9 - Société et Préjugés", french: "Difficultés d'intégration", hebrew: "קְשָׁיֵי קְלִיטָה", transliteration: "kshayé klitah", active: 1 }
];

const run = (sql, params = []) => new Promise((res, rej) => db.run(sql, params, function (err) { if (err) return rej(err); res(this); }));
const get = (sql, params = []) => new Promise((res, rej) => db.get(sql, params, (err, row) => err ? rej(err) : res(row)));

(async () => {
  try {
    const themeName = 'Immigration et Alya';
    const existingTheme = await get('SELECT id FROM themes WHERE name = ? AND user_id IS NULL', [themeName]);
    let themeId = existingTheme ? existingTheme.id : (await run('INSERT INTO themes (name, user_id) VALUES (?, NULL)', [themeName])).lastID;

    await run('UPDATE words SET level_id = NULL WHERE level_id IN (SELECT id FROM theme_levels WHERE theme_id = ?)', [themeId]);
    await run('DELETE FROM words WHERE theme_id = ?', [themeId]);
    await run('DELETE FROM theme_levels WHERE theme_id = ?', [themeId]);

    const levelIds = new Map();
    let order = 0;
    for (const item of data) {
      const levelName = item.level.trim();
      if (!levelIds.has(levelName)) {
        order += 1;
        const ins = await run('INSERT INTO theme_levels (theme_id, name, level_order) VALUES (?,?,?)', [themeId, levelName, order]);
        levelIds.set(levelName, ins.lastID);
      }
    }

    let inserted = 0;
    for (const item of data) {
      const levelId = levelIds.get(item.level.trim());
      const difficulty = item.difficulty ? Number(item.difficulty) : 1;
      const active = item.active ? 1 : 0;
      await run(
        `INSERT INTO words (hebrew, transliteration, french, theme_id, level_id, difficulty, active, user_id)
         VALUES (?,?,?,?,?,?,?,NULL)`,
        [item.hebrew.trim(), (item.transliteration || '').trim() || null, item.french.trim(), themeId, levelId, difficulty, active]
      );
      inserted++;
    }
    console.log(`Done. Inserted ${inserted} words with proper UTF-8.`);
  } catch (e) {
    console.error('Error:', e);
  } finally {
    db.close();
  }
})();

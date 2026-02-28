const express = require('express');
const mysql = require('mysql2');
const cors = require('cors');
const bodyParser = require('body-parser');

const app = express();
const port = 8017;

app.use(bodyParser.json({ limit: '10000mb' }));
app.use(bodyParser.urlencoded({ limit: '10000mb', extended: true }));
app.use(cors());

const DB_CONFIG = {
    host: 'localhost',
    user: 'root',
    password: 'Guoyanjun123.',
    dateStrings: true 
};

let dbName = `terminal_${Date.now()}`;

async function initDatabase() {
    // console.log(`Initializing database: ${dbName}...`);
    const baseDb = mysql.createConnection({
        host: DB_CONFIG.host,
        user: DB_CONFIG.user,
        password: DB_CONFIG.password
    });

    try {
        await new Promise((resolve, reject) => {
            baseDb.connect(err => err ? reject(`Base connection failed: ${err.message}`) : resolve());
        });

        await new Promise((resolve, reject) => {
            baseDb.query(`CREATE DATABASE IF NOT EXISTS \`${dbName}\``, err =>
                err ? reject(`Failed to create database: ${err.message}`) : resolve()
            );
        });

        baseDb.end();

        const db = mysql.createConnection({
            ...DB_CONFIG,
            database: dbName
        });

        await new Promise((resolve, reject) => {
            db.connect(err => err ? reject(`Failed to connect to new database: ${err.message}`) : resolve());
        });

        const createVisitTable = `
            CREATE TABLE IF NOT EXISTS Visit (
                拜访记录编号 VARCHAR(50),
                拜访开始时间 VARCHAR(50),
                拜访结束时间 VARCHAR(50),
                拜访人 VARCHAR(50),
                客户名称 VARCHAR(100),
                客户编码 VARCHAR(50),
                拜访用时 INT,
                INDEX idx_visit_person (拜访人),
                INDEX idx_visit_customer (客户编码)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        `;

        const createTerminalTable = `
            CREATE TABLE IF NOT EXISTS Terminal (
                客户编码 VARCHAR(50),
                客户名称 VARCHAR(100),
                所属片区 VARCHAR(100),
                所属大区 VARCHAR(100),
                UNIQUE INDEX idx_terminal_customer (客户编码)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        `;

        await new Promise((resolve, reject) => {
            db.query(createVisitTable, err => err ? reject(`Failed to create Visit table: ${err.message}`) : resolve());
        });

        await new Promise((resolve, reject) => {
            db.query(createTerminalTable, err => err ? reject(`Failed to create Terminal table: ${err.message}`) : resolve());
        });

        const oldDb = app.get('db');
        if (oldDb) {
            try { oldDb.end(); } catch(e) {}
        }

        app.set('db', db);
        // console.log(`Database initialization completed: ${dbName}`);

    } catch (error) {
        console.error('Database initialization failed:', error);
        if (process.uptime() < 5) {
            process.exit(1);
        }
    }
}

app.post('/api/audit_visit/monthly_end_visits/uploadVisit', (req, res) => {
    const db = app.get('db');
    const records = req.body.records;

    if (!Array.isArray(records) || records.length === 0) {
        return res.status(400).json({ success: false, error: 'Invalid Visit data provided' });
    }

    const values = records.map(r => [
        r.拜访记录编号 || null,
        r.拜访开始时间 || null,
        r.拜访结束时间 || null,
        r.拜访人 || null,
        r.客户名称 || null,
        r.客户编码 || null,
        typeof r.拜访用时 === 'string' ? parseInt(r.拜访用时) || 0 : (r.拜访用时 || 0)
    ]);

    const sql = 'INSERT INTO Visit (拜访记录编号, 拜访开始时间, 拜访结束时间, 拜访人, 客户名称, 客户编码, 拜访用时) VALUES ?';
    db.query(sql, [values], (err, result) => {
        if (err) {
            console.error('Failed to insert Visit records:', err);
            res.status(500).json({ success: false, error: err.message });
        } else {
            res.json({ success: true, message: `${result.affectedRows} records imported` });
        }
    });
});

app.post('/api/audit_visit/monthly_end_visits/uploadTerminal', (req, res) => {
    const db = app.get('db');
    const records = req.body.records;

    if (!Array.isArray(records) || records.length === 0) {
        return res.status(400).json({ success: false, error: 'Invalid Terminal data provided' });
    }

    const values = records.map(r => [
        r.客户编码 || null,
        r.客户名称 || null,
        r.所属片区 || null,
        r.所属大区 || null
    ]);

    const sql = 'INSERT IGNORE INTO Terminal (客户编码, 客户名称, 所属片区, 所属大区) VALUES ?';
    db.query(sql, [values], (err, result) => {
        if (err) {
            res.status(500).json({ success: false, error: err.message });
        } else {
            res.json({ success: true, message: `${result.affectedRows} records imported` });
        }
    });
});

app.get('/api/audit_visit/monthly_end_visits/getLateMonthVisits', (req, res) => {
    const db = app.get('db');

    let {
        targetMonth, 
        minCount = 1,
        visitor = '',
        area = '',
        region = ''
    } = req.query;

    if (!targetMonth) return res.status(400).json({ error: "Missing target month" });

    const threshold = parseInt(minCount) || 1;
    let params = [targetMonth, targetMonth, threshold]; 

    let whereConditions = [];
    if (visitor) { whereConditions.push('f.`拜访人` LIKE ?'); params.push(`%${visitor}%`); }
    if (area) { whereConditions.push('t.所属片区 LIKE ?'); params.push(`%${area}%`); }
    if (region) { whereConditions.push('t.所属大区 LIKE ?'); params.push(`%${region}%`); }

    const whereClause = whereConditions.length > 0 ? 'WHERE ' + whereConditions.join(' AND ') : '';

    const sql = `
        WITH monthly_check AS (
            SELECT
                \`拜访人\`,
                SUM(CASE WHEN DAY(DATE(REPLACE(\`拜访开始时间\`, '/', '-'))) < 25 THEN 1 ELSE 0 END) AS before_25_count
            FROM Visit
            WHERE DATE_FORMAT(DATE(REPLACE(\`拜访开始时间\`, '/', '-')), '%Y-%m') = ?
            GROUP BY \`拜访人\`
        ),
        late_month_stats AS (
            SELECT
                \`拜访人\`,
                SUM(CASE WHEN DAY(DATE(REPLACE(\`拜访开始时间\`, '/', '-'))) = 25 THEN 1 ELSE 0 END) AS day_25,
                SUM(CASE WHEN DAY(DATE(REPLACE(\`拜访开始时间\`, '/', '-'))) = 26 THEN 1 ELSE 0 END) AS day_26,
                SUM(CASE WHEN DAY(DATE(REPLACE(\`拜访开始时间\`, '/', '-'))) = 27 THEN 1 ELSE 0 END) AS day_27,
                SUM(CASE WHEN DAY(DATE(REPLACE(\`拜访开始时间\`, '/', '-'))) = 28 THEN 1 ELSE 0 END) AS day_28,
                SUM(CASE WHEN DAY(DATE(REPLACE(\`拜访开始时间\`, '/', '-'))) = 29 THEN 1 ELSE 0 END) AS day_29,
                SUM(CASE WHEN DAY(DATE(REPLACE(\`拜访开始时间\`, '/', '-'))) = 30 THEN 1 ELSE 0 END) AS day_30,
                SUM(CASE WHEN DAY(DATE(REPLACE(\`拜访开始时间\`, '/', '-'))) = 31 THEN 1 ELSE 0 END) AS day_31,
                COUNT(*) AS total_late_count,
                MAX(\`客户编码\`) AS sample_customer_code
            FROM Visit
            WHERE DATE_FORMAT(DATE(REPLACE(\`拜访开始时间\`, '/', '-')), '%Y-%m') = ?
              AND DAY(DATE(REPLACE(\`拜访开始时间\`, '/', '-'))) >= 25
            GROUP BY \`拜访人\`
        )
        SELECT
            f.\`拜访人\`,
            f.day_25, f.day_26, f.day_27, f.day_28, f.day_29, f.day_30, f.day_31,
            f.total_late_count AS 月末拜访总次数,
            t.所属片区,
            t.所属大区
        FROM late_month_stats f
        JOIN monthly_check c ON f.\`拜访人\` = c.\`拜访人\`
        LEFT JOIN Terminal t ON f.sample_customer_code = t.客户编码
        WHERE c.before_25_count = 0 
          AND f.total_late_count >= ?
        ${whereClause ? ' AND ' + whereClause.replace('WHERE', '') : ''}
        ORDER BY f.total_late_count DESC;
    `;

    db.query(sql, params, (err, results) => {
        if (err) {
            console.error('Failed to query late month visits:', err);
            res.status(500).json({ success: false, error: err.message });
        } else {
            res.json({ success: true, data: results });
        }
    });
});

app.post('/api/audit_visit/monthly_end_visits/cleanup', async (req, res) => {
    // console.log('Manual database cleanup requested...');
    const db = app.get('db');
    try {
        await new Promise((resolve, reject) => {
            db.query('TRUNCATE TABLE Visit', err => err ? reject(err) : resolve());
        });
        await new Promise((resolve, reject) => {
            db.query('TRUNCATE TABLE Terminal', err => err ? reject(err) : resolve());
        });
        // console.log('Database tables cleared successfully');
        res.json({ success: true, message: `Data has been completely cleared.` });
    } catch (error) {
        console.error('Failed to clear database tables:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});

function setupProcessCleanup() {
    async function handleExit(signal) {
        try {
            const db = app.get('db');
            if (db) db.end();
        } catch (error) {}
        process.exit(0);
    }
    process.on('SIGINT', handleExit);
    process.on('SIGTERM', handleExit);
}

initDatabase().then(() => {
    setupProcessCleanup();
    app.listen(port, () => {
        // console.log('='.repeat(60));
        console.log(`Server running on http://localhost:${port}`);
        // console.log(`Current database: ${dbName}`);
        // console.log('API Route: /api/audit_visit/monthly_end_visits');
        // console.log('='.repeat(60));
    });
}).catch(error => {
    console.error('Failed to start server:', error);
    process.exit(1);
});
# CSM Dashboard - Customer Success Management

A modern web application for Customer Success Managers to track account health, renewals, and engagement signals.

## Architecture

- **Frontend**: React 18 + TypeScript + Vite + Tailwind CSS
- **Backend**: FastAPI + Python 3.10+
- **Data**: Databricks SQL (Unity Catalog)
- **Deployment**: Databricks Apps
- **Email notifications**: Scheduled Databricks notebooks call the FastAPI app, which renders Jinja2 HTML and sends mail via SendGrid. Architecture, API parameters, OIDC auth, templates, and testing are documented in [docs/notifications.md](docs/notifications.md).

## Project Structure

```
├── backend/                 # FastAPI backend
│   ├── app/
│   │   ├── api/            # API routes (includes notifications)
│   │   ├── models/         # Pydantic schemas
│   │   ├── services/       # Databricks, email (SendGrid), notifications
│   │   ├── templates/      # Jinja2 HTML email templates
│   │   ├── config.py       # Settings
│   │   └── main.py         # FastAPI app
│   ├── requirements.txt
│   ├── app.yaml            # Databricks Apps config
│   ├── databricks.yml      # Databricks bundle (app + SQL warehouse)
│   └── static/             # Production SPA (from `npm run build:prod`)
├── frontend/               # React frontend
│   ├── src/
│   │   ├── components/     # UI components
│   │   ├── pages/          # Page components
│   │   ├── hooks/          # React Query hooks
│   │   └── services/       # API client
│   └── package.json
├── notebooks/              # Databricks notebooks
│   ├── CustomerHealthScore.py            # Computes daily health scores
│   ├── compute_weekly_summaries.py       # Generates weekly narratives
│   ├── compute_gong_weekly_summaries.py  # Generates Gong call summaries
│   ├── trigger_notification_daily_changes.py   # Triggers daily email run
│   └── trigger_notification_weekly_summary.py  # Triggers weekly email run
├── docs/
│   └── notifications.md    # Full notification system reference
└── README.md
```

## Local Development Setup

### Prerequisites

- Python 3.10+
- Node.js 18+ LTS
- npm or pnpm

### Backend Setup

1. Create and activate virtual environment:
```bash
cd backend
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS/Linux
source .venv/bin/activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create `.env` file (copy from `.env.example`):
```bash
cp .env.example .env
```

4. Configure your Databricks connection in `.env`:
```env
DATABRICKS_HOST=your-workspace.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
DATABRICKS_TOKEN=dapi_your_personal_access_token
ENVIRONMENT=development
```

5. Run the backend:
```bash
uvicorn app.main:app --reload --port 8000
```

The API will be available at http://localhost:8000
- API docs: http://localhost:8000/api/docs
- Health check: http://localhost:8000/api/health

### Frontend Setup

1. Install dependencies:
```bash
cd frontend
npm install
```

2. Run the development server:
```bash
npm run dev
```

The app will be available at http://localhost:5173

### Running Both Together

Open two terminals:

**Terminal 1 - Backend:**
```bash
cd backend
.venv\Scripts\activate  # Windows
uvicorn app.main:app --reload --port 8000
```

**Terminal 2 - Frontend:**
```bash
cd frontend
npm run dev
```

The Vite dev server automatically proxies `/api/*` requests to the backend.

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/health` | Health check |
| GET | `/api/metrics/summary` | Dashboard KPIs |
| GET | `/api/accounts` | List accounts (paginated, filterable) |
| GET | `/api/accounts/{id}` | Account details |
| PATCH | `/api/accounts/{id}/status` | Update account status |
| POST | `/api/tasks` | Create a task |
| POST | `/api/notifications/trigger/weekly-summary` | Send weekly account summary emails (scheduler) |
| POST | `/api/notifications/trigger/daily-changes` | Send daily change digest emails (scheduler) |
| POST | `/api/notifications/test-email` | Verify SendGrid integration |
| GET/POST/PUT/DELETE | `/api/notifications/recipients` | Manage global notification recipients |
| GET | `/api/notifications/preference-keys` | Documented notification opt-out keys |

## Deploying to Databricks Apps

Deploy the **backend** folder: it contains FastAPI, `app.yaml`, `requirements.txt`, email templates under `app/templates/`, and the production SPA under `static/` (after the frontend build).

### 1. Install frontend dependencies and build

```bash
cd frontend
npm install
npm run build:prod
```

`build:prod` runs TypeScript checks and Vite with `--outDir ../backend/static --emptyOutDir`, so `backend/static/` is a clean copy of the SPA for each release.

### 2. Deploy with the Databricks CLI

Log in once (same workspace as in `backend/databricks.yml`):

```bash
databricks auth login --host https://dbc-97a2feb3-3e52.cloud.databricks.com
```

**Option A — Bundle deploy (recommended, matches `backend/databricks.yml`)**

From the repository root:

```bash
cd backend
databricks bundle deploy
```

This deploys the app **`iona-cx`** and attaches the SQL warehouse resource defined in the bundle.

**If `bundle deploy` fails with “An app with the same name already exists”**

The app was created in the UI (or earlier) before the bundle owned it. **Bind** the bundle resource to that app once (resource key `iona_cx` matches `resources.apps.iona_cx` in `databricks.yml`; second argument is the **workspace app name**):

```bash
cd backend
databricks bundle deployment bind iona_cx iona-cx --auto-approve
databricks bundle deploy
```

After binding, `bundle deploy` only **syncs** configuration and uploaded files to the existing app; it does not try to create a second app.

**Option B — Apps deploy (single app, explicit path)**

```bash
databricks apps deploy iona-cx --source-code-path ./backend
```

Use the app name that exists in your workspace (here **`iona-cx`**; adjust if your app was created under another name).

### 3. Configure environment variables and secrets (Databricks App UI)

The app reads configuration from the Databricks Apps environment (and from `app.yaml` for non-secret defaults).

| Variable | Required | Notes |
|----------|----------|--------|
| `ENVIRONMENT` | No | Set to `production` in `app.yaml` |
| `DATABRICKS_WAREHOUSE_ID` | Yes | Provided via bundle resource or set in UI |
| `SENDGRID_API_KEY` | Yes for email | Add in the **App** environment or a **secret scope** — **never** commit real keys to git |
| `SENDGRID_FROM_EMAIL` | No | Default `do-not-reply-iona@ifs.com` in `app.yaml` |
| `SENDGRID_FROM_NAME` | No | Default `Iona CSM` in `app.yaml` |

**SendGrid:** outbound HTTPS to `https://api.sendgrid.com` must be allowed from the app runtime. If IT uses IP allowlisting, allow by FQDN or use SendGrid’s documented egress requirements.

**Key expiry:** `SENDGRID_API_KEY` expires **2026-07-03** (per IT); renew the secret in Databricks only — no code change.

### 4. After deploy — notifications

1. Call **`POST /api/notifications/test-email`** with `{"to_email": "<you@ifs.com>"}` to confirm mail delivery.
2. Add global digest recipients: **`POST /api/notifications/recipients`** (see OpenAPI at `/api/docs` in development, or your API client).
3. Schedule jobs (Databricks Jobs HTTP task or similar) to call:
   - **`POST /api/notifications/trigger/weekly-summary`** (e.g. Monday after weekly summary tables refresh)
   - **`POST /api/notifications/trigger/daily-changes`** (e.g. daily after health score jobs)

Users can opt out of categories via **`PUT /api/preferences/{key}`** with body `{"value":"disabled"}`; keys are listed at **`GET /api/notifications/preference-keys`**.

For the full notification reference — delivery modes, API parameters, auth, templates, and testing — see **[docs/notifications.md](docs/notifications.md)**.

### 5. Deploy without CLI (UI upload)

If the CLI is blocked, use the step-by-step flow in [`DEPLOYMENT.md`](DEPLOYMENT.md) (upload `backend/app`, `backend/static`, `backend/app.yaml`, `backend/requirements.txt`, then **Deploy** from Compute → Apps).

The app uses **Databricks Apps service principal** authentication to SQL; no personal token is required in production.

## Development Notes

### Mock Data

When running locally without a Databricks connection, the backend returns mock data that matches the dashboard mockup. This allows frontend development without a live database.

### Adding New Tables

To connect to your actual Databricks tables:

1. Update the queries in `backend/app/services/databricks.py`
2. Modify the Pydantic schemas in `backend/app/models/schemas.py` if needed
3. Update the mock data to match your schema for local testing

## License

Private - Internal Use Only

# Vercel Deployment Guide

This repository should be deployed as **two separate Vercel projects**:

1. Backend API project (root directory: repository root)
2. Frontend project (root directory: `rubicr-caetis---super-admin`)

## 1) Backend API on Vercel

- Project root: repository root
- Config used: `vercel.json`
- Entry point: `api/index.py` (loads `backend.api.main:app`)

### Required environment variables

- `DATABASE_URL`: Postgres connection string (recommended: Vercel Postgres)
- `FRONTEND_ORIGINS`: comma-separated allowed origins, for example:
  - `https://your-frontend.vercel.app`

### Notes

- SQLite local file storage is not suitable for production serverless environments.
- Use a managed Postgres database.

## 2) Frontend on Vercel

- Project root: `rubicr-caetis---super-admin`
- Config used: `rubicr-caetis---super-admin/vercel.json`

### Required environment variables

- `VITE_API_BASE_URL`: backend API base URL, for example:
  - `https://your-backend.vercel.app`

## 3) Database on Vercel

Use Vercel Postgres:

1. In Vercel dashboard, add Postgres to your backend project.
2. Copy connection string into `DATABASE_URL`.
3. Redeploy backend.

## 4) Verify after deployment

Backend:

- `GET https://<backend-domain>/api/health`

Frontend:

- Open `https://<frontend-domain>`
- Login and run one pipeline job

## 5) Important limitation

Long-running scraping tasks can exceed typical serverless execution windows. For heavy pipeline runs, consider moving worker-style jobs to a background worker platform (Render/Railway/Fly) while keeping frontend on Vercel.

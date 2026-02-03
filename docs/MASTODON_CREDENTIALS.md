# Getting Mastodon API Credentials

The producer needs two things: your **Mastodon instance URL** and an **access token**.

---

## What you need

| Variable | Example | Purpose |
|----------|---------|--------|
| `MASTODON_INSTANCE` | `https://mastodon.social` | The instance base URL (no trailing slash). |
| `MASTODON_ACCESS_TOKEN` | `your_token_here` | OAuth or development token so the app can call the API. |

Without a token you’re limited to a few public endpoints and low rate limits. With a token you can use the **streaming API** and higher limits.

---

## How to get an access token

### Option A — Development token (fastest for local use)

1. **Pick an instance** (e.g. [mastodon.social](https://mastodon.social)) and create an account if you don’t have one.

2. **Create an application:**
   - **Settings** → **Development** → **New application**
   - Name it (e.g. “Sentiment pipeline”).
   - Enable **read** (and optionally **read:statuses**, **streaming** if listed).
   - Submit.

3. **Copy the token:**
   - Open the new app → copy the **Your access token** (sometimes labeled “Access token” or “Access Token”).
   - Put it in `.env` as `MASTODON_ACCESS_TOKEN`.
   - Set `MASTODON_INSTANCE` to your instance URL, e.g. `https://mastodon.social`.

### Option B — OAuth (for production or multi-user)

Use the instance’s OAuth flow (authorize with a user, get a token). For a single-tenant pipeline, the dev token (Option A) is usually enough.

---

## Security

- **Never commit `.env` or your token** — it’s in `.gitignore`.
- In production, use a secrets manager or env injection (e.g. Docker secrets, Kubernetes secrets).
- Rotate the token if it might have been exposed.

---

## References

- [Mastodon API documentation](https://docs.joinmastodon.org/api/)
- [OAuth / app registration](https://docs.joinmastodon.org/client/authorized/)

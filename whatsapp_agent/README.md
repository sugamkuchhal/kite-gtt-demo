# WhatsApp Personal Messaging Agent (Android, Safe Automation)

This module implements a Google Sheets + Apps Script + Web App architecture for **send-ready** WhatsApp messages with a strict human-in-the-loop flow.

## Compliance-first design

- Uses personal WhatsApp only (no Business API).
- No direct WhatsApp API calls.
- No auto-send/background send.
- No WhatsApp Web scraping/automation.
- Final send always happens inside WhatsApp UI.

## Sheets schema

### `contacts_events`
- `id`
- `name`
- `phone` (E.164, individuals)
- `chat_type` (`individual` | `group`)
- `group_invite_link`
- `event_type`
- `event_date` (`YYYY-MM-DD`)
- `relation`
- `language` (`en` | `hi` | `hinglish`)
- `tone` (`warm` | `casual` | `formal` | `fun`)
- `media_mode` (`text` | `image` | `gif` | `manual_photo`)
- `active` (`TRUE` | `FALSE`)

### `festival_calendar`
- `festival`
- `month`
- `day`
- `default_language`
- `default_media`

### `message_templates`
- `event_type`
- `language`
- `tone`
- `template_text` (supports `{{name}}`)

## Files

- `Code.gs`: Apps Script backend, scheduler, message generation, dashboard data endpoint.
- `Dashboard.html`: send-ready dashboard UI.

## Setup

1. Create Google Sheet with the three tabs above.
2. Open **Extensions → Apps Script**.
3. Paste `Code.gs` and `Dashboard.html`.
4. In `Code.gs`, set `SPREADSHEET_ID`.
5. Run `createDailyTrigger()` once to install a 9 AM daily trigger.
6. Deploy as Web App (execute as you, accessible to your Google account).
7. Open the Web App URL on Android and send via WhatsApp buttons.

## Android flow

- 9 AM trigger prepares today's queue in `ready_queue` sheet.
- User opens dashboard from notification/bookmark.
- For each item, user taps button.
- WhatsApp opens with prefilled text (and media URL/copy text for group fallback).
- User manually taps **Send**.

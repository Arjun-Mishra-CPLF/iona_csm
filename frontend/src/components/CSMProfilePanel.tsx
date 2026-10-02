import { useState, useMemo, useCallback, useRef, useEffect, Fragment } from 'react'
import { createPortal } from 'react-dom'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { 
  X, 
  Mail, 
  Phone, 
  MapPin, 
  Calendar, 
  Clock,
  Users,
  DollarSign,
  AlertTriangle,
  Briefcase,
  Building,
  Loader2,
  ArrowRightLeft,
  History,
  User,
  Pencil,
  Trash2,
  Plus,
  BarChart2,
  ExternalLink,
  ChevronRight,
  ChevronDown,
  Download,
  MessageCircle,
  CalendarDays,
  ListFilter,
} from 'lucide-react'
import { clsx } from 'clsx'
import type { CSM } from '../types'
import type { CSMAssignmentRecord } from '../services/api'
import {
  updateCSMAssignmentHistory,
  deleteCSMAssignmentHistory,
  createCSMAssignmentHistory,
  getDistinctCsmNamesFromHistory,
  getCSMs,
  type CSMFeedbackResponseRow,
  type CSMFeedbackByCustomer,
} from '../services/api'
import { useCSMProfile, useCSMAssignmentHistory, useCSMFeedback } from '../hooks/useCSM'

function toDatetimeLocalValue(iso: string | null | undefined): string {
  if (!iso || iso === '-') return ''
  try {
    const d = new Date(iso)
    if (isNaN(d.getTime())) return ''
    const pad = (n: number) => String(n).padStart(2, '0')
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`
  } catch {
    return ''
  }
}

function fromDatetimeLocal(s: string): string {
  return new Date(s).toISOString()
}

function formatCurrency(value: number): string {
  if (value >= 1_000_000) return `€${(value / 1_000_000).toFixed(1)}M`
  if (value >= 1_000) return `€${(value / 1_000).toFixed(0)}K`
  return `€${value.toFixed(0)}`
}

function formatDate(dateStr: string): string {
  try {
    const d = new Date(dateStr)
    if (isNaN(d.getTime())) return dateStr || '—'
    return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
  } catch { return dateStr || '—' }
}

function feedbackPreview(text: string | null | undefined, max = 64): string {
  if (text == null || !String(text).trim()) return '—'
  const t = String(text).trim()
  return t.length <= max ? t : `${t.slice(0, max)}…`
}

function formatTenure(months: number): string {
  const years = Math.floor(months / 12)
  const remainingMonths = months % 12
  if (years === 0) return `${remainingMonths} months`
  if (remainingMonths === 0) return `${years} year${years > 1 ? 's' : ''}`
  return `${years}y ${remainingMonths}m`
}

function formatDays(days: number | null): string {
  if (days == null || days <= 0) return '—'
  if (days < 30) return `${days}d`
  if (days < 365) return `${Math.round(days / 30.44)}mo`
  const y = Math.floor(days / 365)
  const m = Math.round((days % 365) / 30.44)
  return m > 0 ? `${y}y ${m}mo` : `${y}y`
}

// ── NPS tab: filters & derived metrics (client-side) ──
type FeedbackDatePreset = '90d' | '6m' | 'ytd' | 'all'
type FeedbackSourceFilter = 'all' | 'survey' | 'freshdesk'

function isSurveyMonkeySource(source: string | null | undefined): boolean {
  return String(source || '').includes('Survey')
}
function isFreshdeskSource(source: string | null | undefined): boolean {
  return String(source || '').includes('Freshdesk')
}

function getPresetStartDate(preset: FeedbackDatePreset): Date | null {
  const now = new Date()
  if (preset === 'all') return null
  if (preset === '90d') {
    const d = new Date(now)
    d.setDate(d.getDate() - 90)
    d.setHours(0, 0, 0, 0)
    return d
  }
  if (preset === '6m') {
    const d = new Date(now)
    d.setMonth(d.getMonth() - 6)
    d.setHours(0, 0, 0, 0)
    return d
  }
  if (preset === 'ytd') {
    return new Date(now.getFullYear(), 0, 1, 0, 0, 0, 0)
  }
  return null
}

function parseResponseDate(iso: string | null | undefined): Date | null {
  if (!iso) return null
  const d = new Date(iso)
  return isNaN(d.getTime()) ? null : d
}

function filterFeedbackResponses(
  rows: CSMFeedbackResponseRow[],
  preset: FeedbackDatePreset,
  source: FeedbackSourceFilter,
): CSMFeedbackResponseRow[] {
  const start = getPresetStartDate(preset)
  return rows.filter((r) => {
    if (source === 'survey' && !isSurveyMonkeySource(r.source)) return false
    if (source === 'freshdesk' && !isFreshdeskSource(r.source)) return false
    if (start) {
      const rd = parseResponseDate(r.response_date)
      // Missing/unparseable dates: keep row so date filters do not hide all data
      if (rd != null && rd < start) return false
    }
    return true
  })
}

interface FilteredFeedbackSummary {
  total: number
  survey_monkey_count: number
  freshdesk_count: number
  promoters: number
  passives: number
  detractors: number
  uncategorized: number
  nps: number | null
  unique_customers: number
  freshdesk_csat_avg: number | null
  freshdesk_csat_scale: 5 | 10
}

function computeFilteredFeedbackSummary(rows: CSMFeedbackResponseRow[]): FilteredFeedbackSummary {
  let sm = 0
  let fd = 0
  let p = 0
  let pa = 0
  let d = 0
  let u = 0
  const customers = new Set<string>()
  const fdScores: number[] = []
  for (const r of rows) {
    const s = String(r.source || '')
    if (s.includes('Survey')) sm++
    else if (s.includes('Freshdesk')) fd++
    const cat = r.nps_category
    if (cat === 'Promoter') p++
    else if (cat === 'Passive') pa++
    else if (cat === 'Detractor') d++
    else u++
    const cn = (r.customer_name || '').trim()
    if (cn) customers.add(cn)
    if (s.includes('Freshdesk') && r.raw_score != null && !Number.isNaN(Number(r.raw_score))) {
      fdScores.push(Number(r.raw_score))
    }
  }
  const nps_cat_total = p + pa + d
  let nps: number | null = null
  if (nps_cat_total > 0) {
    nps = Math.round(((p - d) / nps_cat_total) * 1000) / 10
  }
  let freshdesk_csat_avg: number | null = null
  let freshdesk_csat_scale: 5 | 10 = 5
  if (fdScores.length > 0) {
    const max = Math.max(...fdScores)
    freshdesk_csat_scale = max > 5 ? 10 : 5
    freshdesk_csat_avg = fdScores.reduce((a, b) => a + b, 0) / fdScores.length
  }
  return {
    total: rows.length,
    survey_monkey_count: sm,
    freshdesk_count: fd,
    promoters: p,
    passives: pa,
    detractors: d,
    uncategorized: u,
    nps,
    unique_customers: customers.size,
    freshdesk_csat_avg,
    freshdesk_csat_scale,
  }
}

function aggregateByCustomerFromResponses(rows: CSMFeedbackResponseRow[]): CSMFeedbackByCustomer[] {
  const byCust: Record<string, CSMFeedbackByCustomer> = {}
  for (const item of rows) {
    const cn = (String(item.customer_name || '')).trim() || 'Unknown'
    const reg = item.region
    if (!byCust[cn]) {
      byCust[cn] = {
        customer_name: cn,
        region: reg,
        count: 0,
        promoters: 0,
        passives: 0,
        detractors: 0,
        last_response_date: null,
      }
    }
    const bc = byCust[cn]
    if (reg && !bc.region) bc.region = reg
    bc.count += 1
    const cat = item.nps_category
    if (cat === 'Promoter') bc.promoters += 1
    else if (cat === 'Passive') bc.passives += 1
    else if (cat === 'Detractor') bc.detractors += 1
    const rdt = item.response_date
    if (rdt && (!bc.last_response_date || String(rdt) > String(bc.last_response_date))) {
      bc.last_response_date = rdt
    }
  }
  return Object.values(byCust).sort((a, b) => (b.count - a.count) || a.customer_name.localeCompare(b.customer_name))
}

function npsQualityLabel(nps: number | null): { label: string; className: string } | null {
  if (nps == null) return null
  if (nps >= 50) return { label: 'EXCELLENT', className: 'bg-emerald-100 text-emerald-900' }
  if (nps >= 30) return { label: 'STRONG', className: 'bg-sky-100 text-sky-900' }
  if (nps >= 0) return { label: 'GOOD', className: 'bg-amber-100 text-amber-900' }
  return { label: 'AT RISK', className: 'bg-rose-100 text-rose-900' }
}

function feedbackScoreBadge(r: CSMFeedbackResponseRow): string {
  if (r.rating_label?.trim()) return r.rating_label.trim()
  if (r.raw_score != null && !Number.isNaN(Number(r.raw_score))) {
    const n = Number(r.raw_score)
    // Only treat as Likert-style score (avoid showing ticket IDs etc. as "103 / 10")
    if (n >= 0 && n <= 10) return `${n} / 10`
    if (n >= 0 && n <= 5) return `${n} / 5`
  }
  if (r.nps_category) return r.nps_category
  return '—'
}

function surveyMonkeyTypeLabel(r: CSMFeedbackResponseRow): string {
  const st = r.response_status?.trim()
  if (st) return st
  const src = String(r.source || '').trim()
  if (src && src !== 'SurveyMonkey') return src
  return 'Survey response'
}

function extractVerbatim(r: CSMFeedbackResponseRow): string | null {
  const fb = r.feedback?.trim()
  if (fb) return fb
  const qs = r.survey_questions
  if (qs && qs.length) {
    let best = ''
    for (const qa of qs) {
      const a = (qa.answer || '').trim()
      if (!a) continue
      const q = (qa.question || '').toLowerCase()
      if (/(story|comment|feedback|why|tell us)/i.test(q) && a.length > best.length) best = a
    }
    if (!best) {
      for (const qa of qs) {
        const a = (qa.answer || '').trim()
        if (a.length > best.length) best = a
      }
    }
    return best || null
  }
  if (r.csat_entries && r.csat_entries.length) {
    const texts = r.csat_entries.map((c) => c.feedback?.trim()).filter(Boolean) as string[]
    if (texts.length) return texts.join(' ')
  }
  return null
}

function respondentInitials(r: CSMFeedbackResponseRow): string {
  const name = (r.respondent_name || r.respondent_email || '').trim()
  if (!name) return '?'
  const parts = name.split(/[\s@]+/).filter(Boolean)
  if (parts.length >= 2) return (parts[0][0] + parts[1][0]).toUpperCase()
  return name.slice(0, 2).toUpperCase()
}

function relativeFeedbackTime(iso: string | null | undefined): string {
  const d = parseResponseDate(iso)
  if (!d) return ''
  const now = new Date()
  const diffMs = now.getTime() - d.getTime()
  const days = Math.floor(diffMs / (24 * 60 * 60 * 1000))
  if (days <= 0) return 'Today'
  if (days === 1) return 'Yesterday'
  if (days < 7) return `${days} days ago`
  if (days < 30) return `${Math.floor(days / 7)} wk ago`
  return formatDate(iso!)
}

function exportFeedbackCsv(rows: CSMFeedbackResponseRow[], filenameBase: string) {
  const headers = ['response_date', 'source', 'customer_name', 'region', 'rating_label', 'nps_category', 'raw_score', 'feedback', 'drill_url']
  const esc = (v: string | number | null | undefined) => {
    const s = v == null ? '' : String(v)
    if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`
    return s
  }
  const lines = [headers.join(',')]
  for (const r of rows) {
    lines.push([
      esc(r.response_date),
      esc(r.source),
      esc(r.customer_name),
      esc(r.region),
      esc(r.rating_label),
      esc(r.nps_category),
      esc(r.raw_score),
      esc(r.feedback),
      esc(r.drill_url),
    ].join(','))
  }
  const blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = `${filenameBase}-feedback.csv`
  a.click()
  URL.revokeObjectURL(url)
}

function feedbackCardCollapsedHint(
  r: CSMFeedbackResponseRow,
  previewText: string | null,
  isFd: boolean,
  isSm: boolean,
): string {
  if (isFd && r.ticket_id != null) {
    const t = String(r.ticket_id)
    const subj = r.ticket_subject && String(r.ticket_subject).trim()
    if (subj) return `Ticket #${t} · ${feedbackPreview(subj, 72)}`
    return `Ticket #${t}`
  }
  if (isSm && r.survey_questions && r.survey_questions.length > 0) {
    const n = r.survey_questions.length
    const st = surveyMonkeyTypeLabel(r)
    return `${st} · ${n} question${n > 1 ? 's' : ''}`
  }
  if (previewText?.trim()) return feedbackPreview(previewText, 100)
  return 'Expand for full details'
}

function FeedbackVerbatimCard({
  r,
  previewText,
}: {
  r: CSMFeedbackResponseRow
  previewText: string | null
}) {
  const [expanded, setExpanded] = useState(false)
  const cat = r.nps_category
  const border =
    cat === 'Promoter'
      ? 'border-l-emerald-500'
      : cat === 'Detractor'
        ? 'border-l-rose-500'
        : cat === 'Passive'
          ? 'border-l-amber-400'
          : 'border-l-slate-300'
  const badge = feedbackScoreBadge(r)
  const isFd = isFreshdeskSource(r.source)
  const isSm = isSurveyMonkeySource(r.source)
  const hasFullFd = Boolean(r.csat_entries && r.csat_entries.length > 0)
  const hasFullSm = Boolean(r.survey_questions && r.survey_questions.length > 0)
  const previewDupesFeedback =
    isFd &&
    Boolean(r.feedback?.trim()) &&
    previewText != null &&
    previewText.trim() === r.feedback!.trim()
  const showItalicPreview =
    Boolean(previewText?.trim()) &&
    !((isFd && hasFullFd) || (isSm && hasFullSm)) &&
    !previewDupesFeedback

  const collapsedHint = feedbackCardCollapsedHint(r, previewText, isFd, isSm)

  return (
    <div
      className={clsx(
        'group relative bg-white rounded-xl border border-slate-200/90 border-l-4 shadow-sm transition-shadow',
        border,
        expanded && 'shadow-md ring-1 ring-slate-200/80',
      )}
    >
      <button
        type="button"
        onClick={() => setExpanded((e) => !e)}
        aria-expanded={expanded}
        className="w-full text-left p-3 rounded-xl hover:bg-slate-50/90 transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-primary-400 focus-visible:ring-offset-1"
      >
        <div className="flex justify-between items-start gap-2">
          <div className="flex flex-wrap items-center gap-2 min-w-0">
            {isFd && (
              <span className="px-2 py-0.5 rounded-md bg-sky-100 text-sky-900 text-[10px] font-bold uppercase tracking-wide">
                Freshdesk
              </span>
            )}
            {isSm && (
              <span className="px-2 py-0.5 rounded-md bg-violet-100 text-violet-900 text-[10px] font-bold uppercase tracking-wide">
                SurveyMonkey
              </span>
            )}
            {!isFd && !isSm && (
              <span className="px-2 py-0.5 rounded-md bg-slate-100 text-slate-700 text-[10px] font-bold uppercase tracking-wide truncate max-w-[200px]">
                {r.source || 'Feedback'}
              </span>
            )}
          </div>
          <div className="flex items-center gap-1.5 shrink-0">
            <span
              className="px-2 py-0.5 bg-slate-100 text-slate-700 text-[10px] rounded-full font-bold max-w-[120px] truncate text-right"
              title={badge}
            >
              {badge}
            </span>
            {expanded ? (
              <ChevronDown className="w-4 h-4 text-slate-400 shrink-0" aria-hidden />
            ) : (
              <ChevronRight className="w-4 h-4 text-slate-400 shrink-0" aria-hidden />
            )}
          </div>
        </div>

        <div className="flex items-start gap-2 mt-2">
          <div className="w-7 h-7 rounded-full bg-slate-100 flex items-center justify-center text-[9px] font-bold text-slate-600 shrink-0">
            {respondentInitials(r)}
          </div>
          <div className="min-w-0 flex-1">
            <p className="text-xs font-bold text-slate-800 truncate">
              {r.respondent_name || r.respondent_email || 'Respondent'}
            </p>
            <p className="text-[10px] text-slate-500 truncate">
              {r.customer_name || '—'}
              {r.region ? ` · ${r.region}` : ''}
            </p>
          </div>
        </div>

        {!expanded && (
          <p className="text-[11px] text-slate-600 mt-2 line-clamp-2 leading-snug">{collapsedHint}</p>
        )}

        <div className="flex flex-wrap items-center justify-between gap-2 mt-2 pt-2 border-t border-slate-100/80">
          <p className="text-[9px] text-slate-400">
            {relativeFeedbackTime(r.response_date)}
            {r.response_date ? ` · ${formatDate(r.response_date)}` : ''}
          </p>
          {r.drill_url && (
            <span
              className="text-[9px] font-semibold text-primary-600"
              onClick={(ev) => ev.stopPropagation()}
            >
              <a
                href={r.drill_url}
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center gap-0.5 hover:underline"
                onClick={(ev) => ev.stopPropagation()}
              >
                {r.drill_label || 'Open'}
                <ExternalLink className="w-3 h-3" />
              </a>
            </span>
          )}
        </div>
      </button>

      {expanded && (
        <div className="px-3 pb-3 border-t border-slate-100">
          <div className="max-h-[min(50vh,380px)] overflow-y-auto overscroll-contain pr-1 pt-3 space-y-3">
            {isFd && (
              <div className="rounded-lg bg-slate-50 border border-slate-200/80 px-3 py-2 text-[11px] text-slate-700 space-y-1">
                <p className="font-semibold text-slate-800">
                  {r.ticket_id != null && r.ticket_id !== ''
                    ? (
                      <>
                        Ticket <span className="tabular-nums font-mono text-primary-700">#{String(r.ticket_id)}</span>
                      </>
                    )
                    : (
                      <span>Ticket (no id)</span>
                    )}
                </p>
                {(r.ticket_subject != null && String(r.ticket_subject).trim()) && (
                  <p>
                    <span className="text-slate-500">Subject</span>{' '}
                    <span className="text-slate-800">{String(r.ticket_subject)}</span>
                  </p>
                )}
                {(r.ticket_status != null && String(r.ticket_status).trim()) && (
                  <p>
                    <span className="text-slate-500">Status</span>{' '}
                    <span>{String(r.ticket_status)}</span>
                  </p>
                )}
              </div>
            )}

            {isSm && (
              <div className="rounded-lg bg-slate-50 border border-slate-200/80 px-3 py-2 text-[11px]">
                <p className="text-slate-500 font-medium uppercase text-[10px] tracking-wide mb-0.5">Survey type</p>
                <p className="text-slate-800 font-medium leading-snug">{surveyMonkeyTypeLabel(r)}</p>
              </div>
            )}

            {showItalicPreview && (
              <p className="text-xs text-slate-600 leading-relaxed italic whitespace-pre-wrap break-words">
                &ldquo;{previewText}&rdquo;
              </p>
            )}

            {isFd && (r.csat_entries && r.csat_entries.length > 0 ? (
              <div className="space-y-3">
                <p className="text-[10px] font-bold text-slate-500 uppercase tracking-wide">All CSAT answers for this ticket</p>
                {r.csat_entries.map((c, idx) => (
                  <div
                    key={idx}
                    className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-[11px] space-y-1"
                  >
                    <div className="flex flex-wrap gap-x-2 gap-y-0.5 text-slate-700">
                      {c.label && <span className="font-semibold text-slate-800">{c.label}</span>}
                      {c.rating_label && (
                        <span className="text-slate-600">{c.rating_label}</span>
                      )}
                      {c.value != null && (
                        <span className="tabular-nums text-slate-500">value: {c.value}</span>
                      )}
                      {c.nps_category && (
                        <span
                          className={clsx(
                            'font-semibold',
                            c.nps_category === 'Promoter' && 'text-emerald-600',
                            c.nps_category === 'Passive' && 'text-amber-600',
                            c.nps_category === 'Detractor' && 'text-rose-600',
                          )}
                        >
                          {c.nps_category}
                        </span>
                      )}
                      {c.response_date && (
                        <span className="text-slate-400">{formatDate(c.response_date)}</span>
                      )}
                    </div>
                    {c.feedback && c.feedback.trim() && (
                      <p className="text-xs text-slate-800 whitespace-pre-wrap break-words pt-1 border-t border-slate-100">
                        {c.feedback}
                      </p>
                    )}
                  </div>
                ))}
              </div>
            ) : isFd && r.feedback && r.feedback.trim() ? (
              <div className="rounded-lg border border-slate-200 bg-slate-50/50 px-3 py-2">
                <p className="text-[10px] font-bold text-slate-500 uppercase tracking-wide mb-1">Feedback</p>
                <p className="text-xs text-slate-800 whitespace-pre-wrap break-words">{r.feedback}</p>
              </div>
            ) : null)}

            {isSm && r.survey_questions && r.survey_questions.length > 0 && (
              <div className="space-y-3">
                <p className="text-[10px] font-bold text-slate-500 uppercase tracking-wide">All survey questions &amp; answers</p>
                {r.survey_questions.map((qa, idx) => (
                  <div key={idx} className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-[11px]">
                    <p className="font-semibold text-slate-700">{qa.question || `Question ${idx + 1}`}</p>
                    <p className="text-sm text-slate-800 mt-1 whitespace-pre-wrap break-words">{qa.answer || '—'}</p>
                  </div>
                ))}
              </div>
            )}

            <div className="flex flex-wrap items-center justify-between gap-2 pt-2 border-t border-slate-100 text-[9px] text-slate-500">
              <span>{isFd ? 'Freshdesk' : isSm ? 'SurveyMonkey' : r.source}</span>
              {r.drill_url && (
                <a
                  href={r.drill_url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-flex items-center gap-1 text-primary-600 hover:text-primary-700 font-semibold"
                >
                  {r.drill_label || 'Open'}
                  <ExternalLink className="w-3 h-3" />
                </a>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

function ProfileStatCard({ 
  icon: Icon, label, value, subValue, color = 'slate'
}: { 
  icon: typeof Users; label: string; value: string | number; subValue?: string
  color?: 'slate' | 'emerald' | 'amber' | 'rose' | 'blue' | 'violet'
}) {
  const colorClasses = {
    slate: 'bg-slate-100 text-slate-600',
    emerald: 'bg-emerald-100 text-emerald-600',
    amber: 'bg-amber-100 text-amber-600',
    rose: 'bg-rose-100 text-rose-600',
    blue: 'bg-blue-100 text-blue-600',
    violet: 'bg-violet-100 text-violet-600',
  }
  return (
    <div className="bg-white rounded-lg border border-slate-200 p-3">
      <div className="flex items-center gap-2 mb-1">
        <div className={clsx('w-6 h-6 rounded flex items-center justify-center', colorClasses[color])}>
          <Icon className="w-3.5 h-3.5" />
        </div>
        <span className="text-xs text-slate-500">{label}</span>
      </div>
      <div className="flex items-baseline gap-2">
        <span className="text-lg font-bold text-slate-800">{value}</span>
        {subValue && <span className="text-xs text-slate-500">{subValue}</span>}
      </div>
    </div>
  )
}

// ── Monthly data point for the chart ──
interface MonthlyPoint {
  label: string
  year: number
  month: number
  cumulative: number
  received: number
  handedOff: number
  net: number
}

function buildMonthlyTimeline(records: CSMAssignmentRecord[]): MonthlyPoint[] {
  if (!records.length) return []

  const parseMonth = (dateStr: string | null): { y: number; m: number } | null => {
    if (!dateStr || dateStr === '-' || dateStr === 'null' || dateStr === 'None') return null
    try {
      const d = new Date(dateStr)
      if (isNaN(d.getTime())) return null
      return { y: d.getFullYear(), m: d.getMonth() + 1 }
    } catch { return null }
  }

  const monthLabels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

  let minDate: { y: number; m: number } | null = null
  const maxDate: { y: number; m: number } = { y: new Date().getFullYear(), m: new Date().getMonth() + 1 }

  for (const r of records) {
    const d = parseMonth(r.assigned_from)
    if (d && (!minDate || d.y < minDate.y || (d.y === minDate.y && d.m < minDate.m))) {
      minDate = d
    }
  }
  if (!minDate) minDate = { y: maxDate.y - 1, m: maxDate.m }

  // Ensure at least 6 months of range so the chart is meaningful
  const minTotal = minDate.y * 12 + minDate.m
  const maxTotal = maxDate.y * 12 + maxDate.m
  if (maxTotal - minTotal < 6) {
    const adjusted = minTotal - (6 - (maxTotal - minTotal))
    minDate = { y: Math.floor((adjusted - 1) / 12), m: ((adjusted - 1) % 12) + 1 }
  }

  const key = (yr: number, mo: number) => `${yr}-${String(mo).padStart(2, '0')}`
  const received: Record<string, number> = {}
  const handedOff: Record<string, number> = {}

  for (const r of records) {
    const d = parseMonth(r.assigned_from)
    if (d) received[key(d.y, d.m)] = (received[key(d.y, d.m)] || 0) + 1

    if (r.status === 'Handed Off') {
      const u = parseMonth(r.assigned_until)
      if (u) handedOff[key(u.y, u.m)] = (handedOff[key(u.y, u.m)] || 0) + 1
    }
  }

  const points: MonthlyPoint[] = []
  let cumulative = 0
  let y = minDate.y, m = minDate.m
  while (y < maxDate.y || (y === maxDate.y && m <= maxDate.m)) {
    const k = key(y, m)
    const recv = received[k] || 0
    const left = handedOff[k] || 0
    cumulative += recv - left
    points.push({
      label: `${monthLabels[m - 1]} ${y}`,
      year: y, month: m,
      cumulative: Math.max(cumulative, 0),
      received: recv, handedOff: left,
      net: recv - left,
    })
    m++
    if (m > 12) { m = 1; y++ }
  }
  return points
}

// ── Assignment Chart (SVG, Customer Growth style) ──
function AssignmentChart({ data, hoveredIndex, onHover }: {
  data: MonthlyPoint[]
  hoveredIndex: number | null
  onHover: (idx: number | null) => void
}) {
  const containerRef = useRef<HTMLDivElement>(null)
  const svgRef = useRef<SVGSVGElement>(null)
  const [tooltipPos, setTooltipPos] = useState<{ x: number; y: number } | null>(null)

  if (data.length < 1) return null

  // Pad single-point data so we can still draw a line
  const chartData = data.length === 1 ? [{ ...data[0], cumulative: 0, received: 0, handedOff: 0, net: 0 }, data[0]] : data

  const W = 900
  const lineH = 180
  const barH = 55
  const gapY = 10
  const totalH = lineH + gapY + barH
  const padX = 30
  const padYTop = 14
  const padYBot = 4

  const values = chartData.map(d => d.cumulative)
  const minVal = Math.min(...values) * 0.95
  const maxVal = Math.max(...values) * 1.05
  const valRange = maxVal - minVal || 1

  const linePoints = chartData.map((_, i) => ({
    x: padX + (i / Math.max(chartData.length - 1, 1)) * (W - 2 * padX),
    y: padYTop + (1 - (values[i] - minVal) / valRange) * (lineH - padYTop - padYBot),
  }))

  let linePath = `M ${linePoints[0].x} ${linePoints[0].y}`
  for (let i = 1; i < linePoints.length; i++) {
    const prev = linePoints[i - 1]
    const curr = linePoints[i]
    const cpx = (prev.x + curr.x) / 2
    linePath += ` C ${cpx} ${prev.y}, ${cpx} ${curr.y}, ${curr.x} ${curr.y}`
  }
  const areaPath = `${linePath} L ${linePoints[linePoints.length - 1].x} ${lineH} L ${linePoints[0].x} ${lineH} Z`

  const segments: { path: string; color: string }[] = []
  for (let i = 0; i < linePoints.length - 1; i++) {
    const prev = linePoints[i]
    const curr = linePoints[i + 1]
    const cpx = (prev.x + curr.x) / 2
    const isUp = chartData[i + 1].net >= 0
    segments.push({
      path: `M ${prev.x} ${prev.y} C ${cpx} ${prev.y}, ${cpx} ${curr.y}, ${curr.x} ${curr.y}`,
      color: isUp ? '#10b981' : '#f43f5e',
    })
  }

  // Y-axis ticks
  const yTicks = useMemo(() => {
    const step = Math.ceil(valRange / 4 / 5) * 5 || 1
    const ticks: number[] = []
    let v = Math.floor(minVal / step) * step
    while (v <= maxVal + step) {
      if (v >= minVal && v <= maxVal) ticks.push(v)
      v += step
    }
    return ticks.length ? ticks : [Math.round(minVal), Math.round(maxVal)]
  }, [minVal, maxVal, valRange])

  // Bar data
  const barTop = lineH + gapY
  const maxBarVal = Math.max(...chartData.map(d => Math.max(d.received, d.handedOff)), 1)
  const barMid = barTop + barH / 2
  const maxBarH = barH / 2 - 4
  const barGap = (W - 2 * padX) / Math.max(chartData.length, 1)
  const barW = Math.min(10, barGap * 0.55)

  const handleMouseMove = useCallback((e: React.MouseEvent<SVGSVGElement>) => {
    if (!svgRef.current || !containerRef.current) return
    const svgRect = svgRef.current.getBoundingClientRect()
    const containerRect = containerRef.current.getBoundingClientRect()
    const relX = e.clientX - svgRect.left
    const ratio = (relX - (padX / W) * svgRect.width) / (svgRect.width * (1 - 2 * padX / W))
    const idx = Math.round(Math.max(0, Math.min(chartData.length - 1, ratio * (chartData.length - 1))))
    if (idx >= 0 && idx < chartData.length) {
      onHover(idx)
      setTooltipPos({ x: e.clientX - containerRect.left, y: e.clientY - containerRect.top })
    }
  }, [chartData.length, onHover])

  const handleMouseLeave = useCallback(() => { onHover(null); setTooltipPos(null) }, [onHover])

  const hovPt = hoveredIndex !== null && hoveredIndex >= 0 && hoveredIndex < chartData.length ? chartData[hoveredIndex] : null

  return (
    <div ref={containerRef} className="relative bg-white rounded-xl border border-slate-200 p-4">
      <div className="flex items-center justify-between mb-2">
        <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wider">Account Portfolio Over Time</h3>
      </div>

      <svg
        ref={svgRef}
        viewBox={`0 0 ${W} ${totalH}`}
        className="w-full"
        style={{ height: 'auto', maxHeight: 260 }}
        onMouseMove={handleMouseMove}
        onMouseLeave={handleMouseLeave}
      >
        {/* Y-axis */}
        {yTicks.map(v => {
          const y = padYTop + (1 - (v - minVal) / valRange) * (lineH - padYTop - padYBot)
          return (
            <g key={v}>
              <line x1={padX} y1={y} x2={W - padX} y2={y} stroke="#f1f5f9" strokeWidth="1" />
              <text x={padX - 6} y={y + 3} textAnchor="end" className="text-[9px] fill-slate-400 font-medium">{v}</text>
            </g>
          )
        })}

        {/* Area + line */}
        <defs>
          <linearGradient id="areaGradCSM" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="#10b981" stopOpacity="0.15" />
            <stop offset="100%" stopColor="#10b981" stopOpacity="0.01" />
          </linearGradient>
        </defs>
        <path d={areaPath} fill="url(#areaGradCSM)" />
        {segments.map((seg, i) => (
          <path key={i} d={seg.path} fill="none" stroke={seg.color} strokeWidth="2.5" strokeLinecap="round" />
        ))}

        {/* Bars */}
        {chartData.map((pt, i) => {
          const cx = padX + (i / Math.max(chartData.length - 1, 1)) * (W - 2 * padX)
          const isHov = hoveredIndex === i
          const recvH = (pt.received / maxBarVal) * maxBarH
          const leftH = (pt.handedOff / maxBarVal) * maxBarH
          return (
            <g key={i}>
              {pt.received > 0 && (
                <rect x={cx - barW / 2} y={barMid - recvH} width={barW} height={recvH} rx={2}
                  fill={isHov ? '#059669' : '#10b981'} opacity={isHov ? 1 : 0.7} />
              )}
              {pt.handedOff > 0 && (
                <rect x={cx - barW / 2} y={barMid} width={barW} height={leftH} rx={2}
                  fill={isHov ? '#e11d48' : '#f43f5e'} opacity={isHov ? 1 : 0.7} />
              )}
            </g>
          )
        })}

        {/* Hover crosshair + dot */}
        {hoveredIndex !== null && linePoints[hoveredIndex] && (
          <>
            <line x1={linePoints[hoveredIndex].x} y1={0} x2={linePoints[hoveredIndex].x} y2={totalH}
              stroke="#94a3b8" strokeWidth="1" strokeDasharray="4 3" />
            <circle cx={linePoints[hoveredIndex].x} cy={linePoints[hoveredIndex].y} r="5"
              fill="white" stroke="#3c83f6" strokeWidth="2.5" />
          </>
        )}
      </svg>

      {/* X-axis labels */}
      <div className="flex justify-between mt-1" style={{ paddingLeft: `${(padX / W) * 100}%`, paddingRight: `${(padX / W) * 100}%` }}>
        {chartData.filter((_, i) => i % Math.max(1, Math.floor(chartData.length / 7)) === 0 || i === chartData.length - 1).map(pt => (
          <span key={`${pt.year}-${pt.month}`} className="text-[9px] text-slate-400 font-medium">{pt.label}</span>
        ))}
      </div>

      {/* Tooltip */}
      {tooltipPos && hovPt && (
        <div className="absolute z-50 pointer-events-none"
          style={{ left: tooltipPos.x, top: Math.max(8, tooltipPos.y - 100), transform: 'translateX(-50%)' }}>
          <div className="bg-slate-900/95 backdrop-blur-sm text-white rounded-xl px-4 py-3 shadow-2xl text-xs min-w-[170px]">
            <div className="flex items-center justify-between mb-1.5">
              <span className="font-bold text-[12px]">{hovPt.label}</span>
              <span className="text-[11px] font-bold text-blue-300">{hovPt.cumulative} accounts</span>
            </div>
            {hovPt.received > 0 && (
              <div className="flex items-center gap-1.5 mb-0.5">
                <div className="w-1.5 h-1.5 rounded-full bg-emerald-400" />
                <span className="text-emerald-400 text-[11px]">+{hovPt.received} received</span>
              </div>
            )}
            {hovPt.handedOff > 0 && (
              <div className="flex items-center gap-1.5 mb-0.5">
                <div className="w-1.5 h-1.5 rounded-full bg-rose-400" />
                <span className="text-rose-400 text-[11px]">-{hovPt.handedOff} handed off</span>
              </div>
            )}
            <div className="flex items-center justify-between pt-1.5 border-t border-slate-700/60 mt-1">
              <span className="text-[10px] text-slate-400 uppercase tracking-wider">Net</span>
              <span className={clsx('font-bold text-[12px]',
                hovPt.net > 0 ? 'text-emerald-400' : hovPt.net < 0 ? 'text-rose-400' : 'text-slate-400'
              )}>{hovPt.net > 0 ? '+' : ''}{hovPt.net}</span>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

type AssignmentDialogState =
  | null
  | { mode: 'add' }
  | { mode: 'edit'; record: CSMAssignmentRecord }

function resolveHandoffIdFromName(name: string, csms: { id: string; name: string }[]): string | null {
  const t = name.trim().toLowerCase()
  if (!t) return null
  const m = csms.find(c => c.name.trim().toLowerCase() === t)
  return m?.id ?? null
}

// ── Events Table ──
function AssignmentEventsTable({ records, statusFilter, onStatusFilter, csmId, csmName }: {
  records: CSMAssignmentRecord[]
  statusFilter: string | null
  onStatusFilter: (f: string | null) => void
  csmId: string
  csmName: string
}) {
  const queryClient = useQueryClient()
  const [dialog, setDialog] = useState<AssignmentDialogState>(null)
  const [form, setForm] = useState({
    account_name: '',
    handed_off_from: '',
    assigned_from: '',
    assigned_until: '',
    openEnded: true,
    status: 'Current' as string,
  })

  const { data: distinctNames = [] } = useQuery({
    queryKey: ['csm-assignment-history-distinct-names'],
    queryFn: getDistinctCsmNamesFromHistory,
    staleTime: 5 * 60 * 1000,
  })

  const { data: csmsResponse } = useQuery({
    queryKey: ['csms', 'handoff-lookup'],
    queryFn: () => getCSMs({}),
    staleTime: 5 * 60 * 1000,
  })
  const csms = csmsResponse?.csms ?? []

  const nameOptions = useMemo(() => {
    const s = new Set<string>(distinctNames)
    for (const r of records) {
      if (r.handed_off_from?.trim()) s.add(r.handed_off_from.trim())
      if (r.csm_name?.trim()) s.add(r.csm_name.trim())
    }
    return Array.from(s).sort((a, b) => a.localeCompare(b))
  }, [distinctNames, records])

  const closeDialog = useCallback(() => setDialog(null), [])

  const updateMut = useMutation({
    mutationFn: updateCSMAssignmentHistory,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['csm-assignment-history', csmId] })
      queryClient.invalidateQueries({ queryKey: ['csm-assignment-history-distinct-names'] })
      closeDialog()
    },
  })

  const createMut = useMutation({
    mutationFn: createCSMAssignmentHistory,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['csm-assignment-history', csmId] })
      queryClient.invalidateQueries({ queryKey: ['csm-assignment-history-distinct-names'] })
      closeDialog()
    },
  })

  const deleteMut = useMutation({
    mutationFn: deleteCSMAssignmentHistory,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['csm-assignment-history', csmId] })
    },
  })

  const openAdd = useCallback(() => {
    setForm({
      account_name: '',
      handed_off_from: '',
      assigned_from: toDatetimeLocalValue(new Date().toISOString()),
      assigned_until: '',
      openEnded: true,
      status: 'Current',
    })
    setDialog({ mode: 'add' })
  }, [])

  const openEdit = useCallback((r: CSMAssignmentRecord) => {
    const open = !r.assigned_until || r.assigned_until === '-' || String(r.assigned_until).trim() === ''
    const isHanded = r.status === 'Handed Off'
    setForm({
      account_name: r.account_name,
      handed_off_from: r.handed_off_from && r.handed_off_from !== 'null' ? r.handed_off_from : '',
      assigned_from: toDatetimeLocalValue(r.assigned_from),
      assigned_until: !open ? toDatetimeLocalValue(r.assigned_until) : '',
      openEnded: open && !isHanded,
      status: isHanded ? 'Handed Off' : 'Current',
    })
    setDialog({ mode: 'edit', record: r })
  }, [])

  const payloadUntilIso = useCallback((): string | null => {
    if (form.status === 'Handed Off') {
      return form.assigned_until.trim() ? fromDatetimeLocal(form.assigned_until) : null
    }
    if (form.status === 'Current') {
      if (form.openEnded) return null
      return form.assigned_until.trim() ? fromDatetimeLocal(form.assigned_until) : null
    }
    return null
  }, [form])

  const save = useCallback(() => {
    const ho = form.handed_off_from.trim()
    const hoId = ho ? resolveHandoffIdFromName(ho, csms) : null
    const untilIso = payloadUntilIso()

    if (form.status === 'Handed Off' && !form.assigned_until.trim()) {
      window.alert('Enter the handoff date when status is Handed Off.')
      return
    }
    if (form.status === 'Current' && !form.openEnded && !form.assigned_until.trim()) {
      window.alert('Enter an end date, or check “still active”.')
      return
    }
    if (!form.assigned_from.trim()) return

    if (dialog?.mode === 'add') {
      const acc = form.account_name.trim()
      if (!acc) {
        window.alert('Enter an account name.')
        return
      }
      createMut.mutate({
        csm_id: csmId,
        csm_name: csmName,
        account_name: acc,
        assigned_from: fromDatetimeLocal(form.assigned_from),
        assigned_until: untilIso,
        handed_off_from: ho || null,
        handed_off_from_id: hoId,
        status: form.status,
      })
      return
    }
    if (dialog?.mode === 'edit' && dialog.record.assigned_from) {
      updateMut.mutate({
        csm_id: csmId,
        account_name: dialog.record.account_name,
        assigned_from_key: dialog.record.assigned_from,
        handed_off_from: ho || null,
        handed_off_from_id: hoId,
        assigned_from: fromDatetimeLocal(form.assigned_from),
        assigned_until: untilIso,
        status: form.status,
      })
    }
  }, [form, dialog, csmId, csmName, csms, payloadUntilIso, createMut, updateMut])

  const confirmDelete = useCallback((r: CSMAssignmentRecord) => {
    if (!r.assigned_from) return
    if (!window.confirm(`Remove assignment history for "${r.account_name}"? This cannot be undone.`)) return
    deleteMut.mutate({ csm_id: csmId, account_name: r.account_name, assigned_from: r.assigned_from })
  }, [csmId, deleteMut])

  const filtered = useMemo(() => {
    if (!statusFilter) return records
    return records.filter(r => r.status === statusFilter)
  }, [records, statusFilter])

  const pending = updateMut.isPending || createMut.isPending

  const modal = dialog && (
    <div
      className="fixed inset-0 z-[100] flex items-center justify-center p-4 bg-slate-900/40"
      onClick={closeDialog}
      role="presentation"
    >
      <div
        role="dialog"
        aria-modal
        className="bg-white rounded-xl shadow-2xl border border-slate-200 p-5 max-w-lg w-full max-h-[90vh] overflow-y-auto"
        onClick={(e) => e.stopPropagation()}
      >
        <h4 className="text-sm font-semibold text-slate-800 mb-1 truncate">
          {dialog.mode === 'add' ? 'Add assignment' : dialog.record.account_name}
        </h4>
        <p className="text-[10px] text-slate-400 mb-4">
          {dialog.mode === 'add'
            ? 'Create a row in assignment history for this CSM.'
            : 'Update this row in the assignment history table.'}
        </p>
        <div className="space-y-3 text-left">
          {dialog.mode === 'add' && (
            <label className="block">
              <span className="text-[10px] font-semibold text-slate-500 uppercase">Account</span>
              <input
                type="text"
                value={form.account_name}
                onChange={(e) => setForm(f => ({ ...f, account_name: e.target.value }))}
                className="mt-1 w-full px-2 py-1.5 text-sm border border-slate-200 rounded-lg"
                placeholder="Account name"
              />
            </label>
          )}
          <label className="block">
            <span className="text-[10px] font-semibold text-slate-500 uppercase">Handed off from</span>
            <select
              value={form.handed_off_from}
              onChange={(e) => setForm(f => ({ ...f, handed_off_from: e.target.value }))}
              className="mt-1 w-full px-2 py-1.5 text-sm border border-slate-200 rounded-lg bg-white"
            >
              <option value="">— None —</option>
              {nameOptions.map(n => (
                <option key={n} value={n}>{n}</option>
              ))}
            </select>
          </label>
          <label className="block">
            <span className="text-[10px] font-semibold text-slate-500 uppercase">Assigned from</span>
            <input
              type="datetime-local"
              value={form.assigned_from}
              onChange={(e) => setForm(f => ({ ...f, assigned_from: e.target.value }))}
              className="mt-1 w-full px-2 py-1.5 text-sm border border-slate-200 rounded-lg"
            />
          </label>
          <label className="block">
            <span className="text-[10px] font-semibold text-slate-500 uppercase">Status</span>
            <select
              value={form.status}
              onChange={(e) => {
                const v = e.target.value
                setForm(f => ({
                  ...f,
                  status: v,
                  openEnded: v === 'Handed Off' ? false : f.openEnded,
                  assigned_until: v === 'Handed Off' && !f.assigned_until
                    ? toDatetimeLocalValue(new Date().toISOString())
                    : f.assigned_until,
                }))
              }}
              className="mt-1 w-full px-2 py-1.5 text-sm border border-slate-200 rounded-lg bg-white"
            >
              <option value="Current">Current</option>
              <option value="Handed Off">Handed Off</option>
            </select>
          </label>
          {form.status === 'Current' && (
            <label className="flex items-center gap-2 cursor-pointer">
              <input
                type="checkbox"
                checked={form.openEnded}
                onChange={(e) =>
                  setForm(f => ({ ...f, openEnded: e.target.checked, assigned_until: e.target.checked ? '' : f.assigned_until }))
                }
                className="rounded border-slate-300"
              />
              <span className="text-xs text-slate-600">No end date (still active / present)</span>
            </label>
          )}
          {form.status === 'Handed Off' && (
            <label className="block">
              <span className="text-[10px] font-semibold text-slate-500 uppercase">Handoff date</span>
              <input
                type="datetime-local"
                value={form.assigned_until}
                onChange={(e) => setForm(f => ({ ...f, assigned_until: e.target.value }))}
                className="mt-1 w-full px-2 py-1.5 text-sm border border-slate-200 rounded-lg"
              />
              <p className="text-[10px] text-slate-400 mt-1">When this assignment ended (required for Handed Off).</p>
            </label>
          )}
          {form.status === 'Current' && !form.openEnded && (
            <label className="block">
              <span className="text-[10px] font-semibold text-slate-500 uppercase">Assigned until</span>
              <input
                type="datetime-local"
                value={form.assigned_until}
                onChange={(e) => setForm(f => ({ ...f, assigned_until: e.target.value }))}
                className="mt-1 w-full px-2 py-1.5 text-sm border border-slate-200 rounded-lg"
              />
            </label>
          )}
        </div>
        {(updateMut.isError || createMut.isError) && (
          <p className="text-xs text-rose-600 mt-2">Save failed. Check permissions or duplicate row.</p>
        )}
        <div className="flex justify-end gap-2 mt-5">
          <button type="button" onClick={closeDialog} className="px-3 py-1.5 text-sm text-slate-600 hover:bg-slate-100 rounded-lg">
            Cancel
          </button>
          <button
            type="button"
            onClick={save}
            disabled={pending || !form.assigned_from}
            className="px-3 py-1.5 text-sm font-medium bg-primary-600 text-white rounded-lg hover:bg-primary-700 disabled:opacity-50"
          >
            {pending ? 'Saving…' : 'Save'}
          </button>
        </div>
      </div>
    </div>
  )

  return (
    <>
      {typeof document !== 'undefined' && modal && createPortal(modal, document.body)}
      <div className="bg-white rounded-xl border border-slate-200 overflow-hidden">
      <div className="px-4 py-3 border-b border-slate-100">
        <div className="flex items-center justify-between mb-2 gap-2">
          <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wider">Assignment Events</h3>
          <div className="flex items-center gap-2 shrink-0">
            <span className="text-[10px] text-slate-400 font-medium">
              {statusFilter ? `${filtered.length} of ${records.length}` : `${records.length}`} events
            </span>
            <button
              type="button"
              onClick={openAdd}
              className="inline-flex items-center gap-1 px-2 py-1 rounded-lg text-[10px] font-semibold bg-primary-50 text-primary-700 border border-primary-200 hover:bg-primary-100"
            >
              <Plus className="w-3 h-3" />
              Add
            </button>
          </div>
        </div>
        <div className="flex items-center gap-2">
          {['Current', 'Handed Off'].map(s => (
            <button key={s} onClick={() => onStatusFilter(statusFilter === s ? null : s)}
              className={clsx(
                'px-2.5 py-1 rounded-lg text-[10px] font-semibold border transition-all',
                statusFilter === s
                  ? s === 'Current' ? 'bg-emerald-50 border-emerald-200 text-emerald-700' : 'bg-slate-100 border-slate-300 text-slate-600'
                  : 'bg-slate-50 border-slate-200 text-slate-500 hover:border-slate-300'
              )}>
              {s}
            </button>
          ))}
          {statusFilter && (
            <button onClick={() => onStatusFilter(null)}
              className="text-[10px] text-slate-400 hover:text-primary-600 font-medium px-1.5">
              Clear
            </button>
          )}
        </div>
      </div>

      <div className="grid grid-cols-[auto_24px_1fr_90px_70px_90px_76px] items-center gap-2 px-4 py-2 bg-slate-50/80 border-b border-slate-100 text-[9px] font-bold uppercase tracking-wider text-slate-400">
        <span className="w-5"></span>
        <span></span>
        <span>Account</span>
        <span>Status</span>
        <span className="text-right">Duration</span>
        <span className="text-right">Handed From</span>
        <span className="text-right">Actions</span>
      </div>

      <div className="max-h-80 overflow-y-auto divide-y divide-slate-50">
        {filtered.map((r, i) => {
          const isCurrent = r.status === 'Current'
          return (
            <div key={`${r.account_name}-${r.assigned_from}-${i}`}
              className="grid grid-cols-[auto_24px_1fr_90px_70px_90px_76px] items-center gap-2 px-4 py-2 hover:bg-slate-50/50 transition-colors">
              <span className="text-[9px] text-slate-300 font-medium w-5 text-right">{i + 1}</span>
              <div className="flex justify-center">
                <div className={clsx('w-2 h-2 rounded-full', isCurrent ? 'bg-emerald-500' : 'bg-slate-400')} />
              </div>
              <div className="min-w-0">
                <p className="text-[12px] font-medium text-slate-800 truncate">{r.account_name}</p>
                <p className="text-[10px] text-slate-400 truncate">
                  {r.assigned_from ? formatDate(r.assigned_from) : '—'}
                  {r.assigned_until && r.assigned_until !== '-' ? ` → ${formatDate(r.assigned_until)}` : ' → present'}
                </p>
              </div>
              <div>
                <span className={clsx(
                  'text-[9px] font-bold uppercase tracking-wider px-2 py-0.5 rounded-full',
                  isCurrent ? 'bg-emerald-50 text-emerald-600' : 'bg-slate-100 text-slate-500'
                )}>
                  {r.status}
                </span>
              </div>
              <span className="text-[11px] text-slate-500 font-medium text-right">{formatDays(r.days_held)}</span>
              <span className="text-[10px] text-slate-400 text-right truncate" title={r.handed_off_from || ''}>
                {r.handed_off_from || '—'}
              </span>
              <div className="flex items-center justify-end gap-1">
                <button
                  type="button"
                  onClick={() => openEdit(r)}
                  className="p-1.5 rounded-lg text-slate-500 hover:bg-primary-50 hover:text-primary-600"
                  title="Edit"
                >
                  <Pencil className="w-3.5 h-3.5" />
                </button>
                <button
                  type="button"
                  onClick={() => confirmDelete(r)}
                  disabled={deleteMut.isPending}
                  className="p-1.5 rounded-lg text-slate-500 hover:bg-rose-50 hover:text-rose-600 disabled:opacity-50"
                  title="Delete"
                >
                  <Trash2 className="w-3.5 h-3.5" />
                </button>
              </div>
            </div>
          )
        })}
      </div>
      {filtered.length === 0 && (
        <div className="px-4 py-8 text-center text-sm text-slate-400">
          {records.length === 0
            ? 'No assignment history yet. Use Add to create a row.'
            : 'No rows match this filter.'}
        </div>
      )}
    </div>
    </>
  )
}

// ── Main Panel ──
type PanelTab = 'profile' | 'history' | 'feedback'

interface CSMProfilePanelProps {
  csm: CSM
  isOpen: boolean
  onClose: () => void
}

export function CSMProfilePanel({ csm, isOpen, onClose }: CSMProfilePanelProps) {
  const [activeTab, setActiveTab] = useState<PanelTab>('profile')
  const [hoveredChartIdx, setHoveredChartIdx] = useState<number | null>(null)
  const [statusFilter, setStatusFilter] = useState<string | null>(null)
  const [expandedFeedbackKeys, setExpandedFeedbackKeys] = useState<Set<string>>(() => new Set())
  const [feedbackDatePreset, setFeedbackDatePreset] = useState<FeedbackDatePreset>('all')
  const [feedbackSourceFilter, setFeedbackSourceFilter] = useState<FeedbackSourceFilter>('all')

  const { data: profile, isLoading: profileLoading, isError: profileError } = useCSMProfile(isOpen ? csm.id : null)
  const { data: historyData, isLoading: historyLoading } = useCSMAssignmentHistory(isOpen ? csm.id : null)
  const { data: feedbackData, isLoading: feedbackLoading, isError: feedbackError } = useCSMFeedback(
    isOpen ? csm.id : null,
    isOpen && activeTab === 'feedback',
  )

  useEffect(() => {
    setExpandedFeedbackKeys(new Set())
    setFeedbackDatePreset('all')
    setFeedbackSourceFilter('all')
  }, [csm.id, isOpen])

  const toggleFeedbackRow = useCallback((key: string) => {
    setExpandedFeedbackKeys((prev) => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }, [])

  const isDeparted = csm.status === 'departed'
  const isInactive = csm.status === 'inactive'

  const displayProfile = useMemo(() => {
    if (!profile) return null
    return {
      title: profile.title || 'Customer Success Manager',
      department: profile.department || 'Customer Success',
      phone: profile.phone || profile.mobile_phone,
      location: profile.location,
      reports_to: profile.reports_to,
      joined_date: profile.joined_date,
      tenure_months: profile.tenure_months,
      email: profile.email || csm.email,
      region: profile.region,
      timezone: profile.timezone,
    }
  }, [profile, csm.email])

  const timeline = useMemo(() => buildMonthlyTimeline(historyData || []), [historyData])

  const filteredFeedbackResponses = useMemo(() => {
    if (!feedbackData?.responses) return []
    return filterFeedbackResponses(feedbackData.responses, feedbackDatePreset, feedbackSourceFilter)
  }, [feedbackData?.responses, feedbackDatePreset, feedbackSourceFilter])

  const filteredFeedbackSummary = useMemo(
    () => computeFilteredFeedbackSummary(filteredFeedbackResponses),
    [filteredFeedbackResponses],
  )

  const filteredByCustomer = useMemo(
    () => aggregateByCustomerFromResponses(filteredFeedbackResponses),
    [filteredFeedbackResponses],
  )

  const recentVerbatimRows = useMemo(() => {
    const scored = filteredFeedbackResponses.map((r) => ({
      r,
      text: extractVerbatim(r),
    }))
    const keep = scored.filter(({ r, text }) => {
      if (text && text.trim().length > 0) return true
      if (isFreshdeskSource(r.source) && (r.ticket_id != null || (r.csat_entries && r.csat_entries.length > 0)))
        return true
      if (isSurveyMonkeySource(r.source) && r.survey_questions && r.survey_questions.length > 0) return true
      return false
    })
    keep.sort((a, b) => {
      const ta = parseResponseDate(a.r.response_date)?.getTime() ?? 0
      const tb = parseResponseDate(b.r.response_date)?.getTime() ?? 0
      return tb - ta
    })
    return keep.slice(0, 8)
  }, [filteredFeedbackResponses])

  const npsQualityBadge = useMemo(
    () => npsQualityLabel(filteredFeedbackSummary.nps),
    [filteredFeedbackSummary.nps],
  )

  const historySummary = useMemo(() => {
    if (!historyData || !historyData.length) return null
    const current = historyData.filter(r => r.status === 'Current').length
    const totalManaged = new Set(historyData.map(r => r.account_name)).size
    const handedOff = historyData.filter(r => r.status === 'Handed Off').length
    const received = historyData.filter(r => r.handed_off_from && r.handed_off_from !== 'null').length
    const daysArr = historyData.filter(r => r.days_held != null && r.days_held > 0).map(r => r.days_held!)
    const avgDays = daysArr.length ? Math.round(daysArr.reduce((a, b) => a + b, 0) / daysArr.length) : 0
    return { current, totalManaged, handedOff, received, avgDays }
  }, [historyData])

  if (!isOpen) return null

  return (
    <>
      <div className="fixed inset-0 top-[57px] bg-black/20 z-40 transition-opacity" onClick={onClose} />

      <div className="fixed right-0 top-[57px] bottom-0 w-full max-w-5xl bg-white shadow-2xl z-50 overflow-hidden flex flex-col animate-slide-in-right">
        {/* Header */}
        <div className={clsx(
          'px-6 py-4 border-b flex items-start justify-between',
          isDeparted ? 'bg-rose-50 border-rose-200' :
          isInactive ? 'bg-slate-50 border-slate-200' :
          'bg-gradient-to-r from-primary-50 to-primary-100 border-primary-200'
        )}>
          <div className="flex items-center gap-4">
            <div className={clsx(
              'w-14 h-14 rounded-full flex items-center justify-center text-white text-xl font-bold',
              isDeparted ? 'bg-slate-400' : isInactive ? 'bg-slate-400' : 'bg-gradient-to-br from-primary-500 to-primary-700'
            )}>
              {csm.name.split(' ').map(n => n[0]).join('')}
            </div>
            <div>
              <div className="flex items-center gap-2">
                <h2 className={clsx('text-xl font-bold', isDeparted ? 'text-slate-500 line-through' : 'text-slate-800')}>
                  {csm.name}
                </h2>
                {isDeparted && <span className="px-2 py-0.5 bg-rose-100 text-rose-700 text-xs font-medium rounded-full">Departed</span>}
                {isInactive && <span className="px-2 py-0.5 bg-slate-200 text-slate-600 text-xs font-medium rounded-full">Inactive</span>}
              </div>
              {profileLoading ? (
                <div className="h-4 w-40 bg-slate-200 rounded animate-pulse mt-1" />
              ) : (
                <>
                  <p className="text-sm text-slate-500">{displayProfile?.title}</p>
                  <div className="flex items-center gap-3 mt-1 text-xs text-slate-500">
                    <span className="flex items-center gap-1"><Briefcase className="w-3 h-3" />{displayProfile?.department}</span>
                    {displayProfile?.tenure_months != null && (
                      <span className="flex items-center gap-1"><Clock className="w-3 h-3" />{formatTenure(displayProfile.tenure_months)} tenure</span>
                    )}
                  </div>
                </>
              )}
            </div>
          </div>
          <button onClick={onClose} className="p-2 text-slate-400 hover:text-slate-600 hover:bg-white/50 rounded-lg transition-colors">
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Tabs */}
        <div className="flex border-b border-slate-200 bg-white px-6">
          <button
            onClick={() => setActiveTab('profile')}
            className={clsx(
              'flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors -mb-px',
              activeTab === 'profile'
                ? 'border-primary-500 text-primary-600'
                : 'border-transparent text-slate-500 hover:text-slate-700'
            )}>
            <User className="w-3.5 h-3.5" />
            Profile
          </button>
          <button
            onClick={() => setActiveTab('history')}
            className={clsx(
              'flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors -mb-px',
              activeTab === 'history'
                ? 'border-primary-500 text-primary-600'
                : 'border-transparent text-slate-500 hover:text-slate-700'
            )}>
            <History className="w-3.5 h-3.5" />
            Assignment History
            {historyData && historyData.length > 0 && (
              <span className="ml-1 px-1.5 py-0.5 bg-slate-100 text-slate-500 text-[10px] font-bold rounded-full">
                {historyData.length}
              </span>
            )}
          </button>
          <button
            onClick={() => setActiveTab('feedback')}
            className={clsx(
              'flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors -mb-px',
              activeTab === 'feedback'
                ? 'border-primary-500 text-primary-600'
                : 'border-transparent text-slate-500 hover:text-slate-700'
            )}>
            <BarChart2 className="w-3.5 h-3.5" />
            NPS &amp; satisfaction
            {feedbackData && feedbackData.summary.total > 0 && (
              <span className="ml-1 px-1.5 py-0.5 bg-slate-100 text-slate-500 text-[10px] font-bold rounded-full">
                {feedbackData.summary.total}
              </span>
            )}
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto">
          {activeTab === 'profile' ? (
            /* ── Profile Tab ── */
            profileLoading ? (
              <div className="flex items-center justify-center py-16">
                <Loader2 className="w-6 h-6 text-primary-500 animate-spin" />
                <span className="ml-2 text-sm text-slate-500">Loading profile...</span>
              </div>
            ) : profileError ? (
              <div className="px-6 py-8 text-center">
                <p className="text-sm text-slate-500">Could not load profile data.</p>
              </div>
            ) : (
              <>
                <div className="px-6 py-4 border-b border-slate-100">
                  <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-3">Contact Information</h3>
                  <div className="grid grid-cols-2 gap-3">
                    {displayProfile?.email && (
                      <div className="flex items-center gap-2 text-sm">
                        <Mail className="w-4 h-4 text-slate-400 shrink-0" />
                        <span className="text-slate-600 truncate">{displayProfile.email}</span>
                      </div>
                    )}
                    {displayProfile?.phone && (
                      <div className="flex items-center gap-2 text-sm">
                        <Phone className="w-4 h-4 text-slate-400 shrink-0" />
                        <span className="text-slate-600">{displayProfile.phone}</span>
                      </div>
                    )}
                    {displayProfile?.location && (
                      <div className="flex items-center gap-2 text-sm">
                        <MapPin className="w-4 h-4 text-slate-400 shrink-0" />
                        <span className="text-slate-600">{displayProfile.location}</span>
                      </div>
                    )}
                    {displayProfile?.reports_to && (
                      <div className="flex items-center gap-2 text-sm">
                        <Building className="w-4 h-4 text-slate-400 shrink-0" />
                        <span className="text-slate-600">Reports to {displayProfile.reports_to}</span>
                      </div>
                    )}
                    {displayProfile?.joined_date && (
                      <div className="flex items-center gap-2 text-sm">
                        <Calendar className="w-4 h-4 text-slate-400 shrink-0" />
                        <span className="text-slate-600">Joined {formatDate(displayProfile.joined_date)}</span>
                      </div>
                    )}
                    {displayProfile?.region && (
                      <div className="flex items-center gap-2 text-sm">
                        <MapPin className="w-4 h-4 text-slate-400 shrink-0" />
                        <span className="text-slate-600">{displayProfile.region}</span>
                      </div>
                    )}
                  </div>
                </div>

                <div className="px-6 py-4 border-b border-slate-100">
                  <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-3">
                    {isDeparted ? 'Final Portfolio' : 'Current Portfolio'}
                  </h3>
                  <div className="grid grid-cols-2 gap-3">
                    <ProfileStatCard icon={Users} label="Accounts" value={csm.account_count} color="blue" />
                    <ProfileStatCard icon={DollarSign} label="ARR Managed" value={formatCurrency(csm.total_arr)} color="emerald" />
                    <ProfileStatCard icon={AlertTriangle} label="Needs Attention" value={csm.at_risk_count} color={csm.at_risk_count > 2 ? 'rose' : 'amber'} />
                    <ProfileStatCard icon={Calendar} label="Renewals (90d)" value={Math.floor(csm.account_count * 0.2)} color="violet" />
                  </div>
                </div>
              </>
            )
          ) : activeTab === 'history' ? (
            /* ── Assignment History Tab ── */
            historyLoading ? (
              <div className="flex items-center justify-center py-16">
                <Loader2 className="w-6 h-6 text-primary-500 animate-spin" />
                <span className="ml-2 text-sm text-slate-500">Loading history...</span>
              </div>
            ) : (
              <div className="p-4 space-y-4">
                {/* Summary cards */}
                {historySummary && (
                  <div className="grid grid-cols-4 gap-3">
                    <div className="bg-white rounded-xl border border-slate-200 p-3 hover:shadow-sm transition-shadow">
                      <div className="flex items-center gap-2">
                        <div className="w-7 h-7 rounded-lg flex items-center justify-center bg-blue-50 text-blue-600">
                          <Users className="w-3.5 h-3.5" />
                        </div>
                        <div>
                          <p className="text-[10px] text-slate-500">Current</p>
                          <p className="text-base font-bold text-slate-800">{historySummary.current}</p>
                        </div>
                      </div>
                    </div>
                    <div className="bg-white rounded-xl border border-slate-200 p-3 hover:shadow-sm transition-shadow">
                      <div className="flex items-center gap-2">
                        <div className="w-7 h-7 rounded-lg flex items-center justify-center bg-emerald-50 text-emerald-600">
                          <Briefcase className="w-3.5 h-3.5" />
                        </div>
                        <div>
                          <p className="text-[10px] text-slate-500">All-Time</p>
                          <p className="text-base font-bold text-slate-800">{historySummary.totalManaged}</p>
                        </div>
                      </div>
                    </div>
                    <div className="bg-white rounded-xl border border-slate-200 p-3 hover:shadow-sm transition-shadow">
                      <div className="flex items-center gap-2">
                        <div className="w-7 h-7 rounded-lg flex items-center justify-center bg-amber-50 text-amber-600">
                          <Clock className="w-3.5 h-3.5" />
                        </div>
                        <div>
                          <p className="text-[10px] text-slate-500">Avg Tenure</p>
                          <p className="text-base font-bold text-slate-800">{formatDays(historySummary.avgDays)}</p>
                        </div>
                      </div>
                    </div>
                    <div className="bg-white rounded-xl border border-slate-200 p-3 hover:shadow-sm transition-shadow">
                      <div className="flex items-center gap-2">
                        <div className="w-7 h-7 rounded-lg flex items-center justify-center bg-violet-50 text-violet-600">
                          <ArrowRightLeft className="w-3.5 h-3.5" />
                        </div>
                        <div>
                          <p className="text-[10px] text-slate-500">Transfers</p>
                          <p className="text-base font-bold text-slate-800">
                            <span className="text-emerald-600">{historySummary.received}</span>
                            <span className="text-slate-400 mx-0.5">/</span>
                            <span className="text-slate-500">{historySummary.handedOff}</span>
                          </p>
                        </div>
                      </div>
                    </div>
                  </div>
                )}

                {/* Chart */}
                {timeline.length >= 1 && (
                  <AssignmentChart data={timeline} hoveredIndex={hoveredChartIdx} onHover={setHoveredChartIdx} />
                )}

                {/* Events table */}
                <AssignmentEventsTable
                  records={historyData || []}
                  statusFilter={statusFilter}
                  onStatusFilter={setStatusFilter}
                  csmId={csm.id}
                  csmName={csm.name}
                />
              </div>
            )
          ) : (
            /* ── NPS & satisfaction Tab ── */
            feedbackLoading ? (
              <div className="flex items-center justify-center py-16">
                <Loader2 className="w-6 h-6 text-primary-500 animate-spin" />
                <span className="ml-2 text-sm text-slate-500">Loading feedback...</span>
              </div>
            ) : feedbackError ? (
              <div className="px-6 py-8 text-center">
                <p className="text-sm text-slate-500">Could not load NPS and satisfaction data.</p>
              </div>
            ) : (
              <div className="p-5 pb-8 space-y-8">
                <p className="text-[11px] text-slate-500 leading-relaxed">
                  SurveyMonkey NPS-style question and Freshdesk CSAT for accounts where this CSM is the owner on{' '}
                  <span className="font-medium text-slate-600">dim_customers</span>. Configure{' '}
                  <code className="text-[10px] bg-slate-100 px-1 rounded font-mono">FRESHDESK_PORTAL_BASE</code> and{' '}
                  <code className="text-[10px] bg-slate-100 px-1 rounded font-mono">SURVEY_MONKEY_RESPONSE_URL_TEMPLATE</code>{' '}
                  for ticket/survey links. By default, filters show <span className="font-medium text-slate-600">all time</span> and{' '}
                  <span className="font-medium text-slate-600">both SurveyMonkey and Freshdesk</span>; narrow the dropdowns if needed.
                </p>

                {feedbackData && (
                  <>
                    <div className="flex flex-col sm:flex-row gap-3">
                      <div className="flex-1 relative min-w-0">
                        <CalendarDays className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400 pointer-events-none" />
                        <select
                          value={feedbackDatePreset}
                          onChange={(e) => setFeedbackDatePreset(e.target.value as FeedbackDatePreset)}
                          className="w-full pl-10 pr-3 py-2.5 bg-slate-50 border border-slate-200/80 rounded-xl text-xs font-medium text-slate-700 focus:ring-2 focus:ring-primary-500/20 focus:border-primary-300"
                        >
                          <option value="all">All time</option>
                          <option value="90d">Last 90 days</option>
                          <option value="6m">Last 6 months</option>
                          <option value="ytd">Year to date</option>
                        </select>
                      </div>
                      <div className="flex-1 relative min-w-0">
                        <ListFilter className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400 pointer-events-none" />
                        <select
                          value={feedbackSourceFilter}
                          onChange={(e) => setFeedbackSourceFilter(e.target.value as FeedbackSourceFilter)}
                          className="w-full pl-10 pr-3 py-2.5 bg-slate-50 border border-slate-200/80 rounded-xl text-xs font-medium text-slate-700 focus:ring-2 focus:ring-primary-500/20 focus:border-primary-300"
                        >
                          <option value="all">SurveyMonkey + Freshdesk</option>
                          <option value="survey">SurveyMonkey only</option>
                          <option value="freshdesk">Freshdesk only</option>
                        </select>
                      </div>
                      <button
                        type="button"
                        onClick={() => exportFeedbackCsv(filteredFeedbackResponses, `csm-${csm.id}`)}
                        disabled={filteredFeedbackResponses.length === 0}
                        className="inline-flex items-center justify-center gap-2 px-4 py-2.5 rounded-xl bg-primary-600 text-white text-xs font-semibold hover:bg-primary-700 disabled:opacity-40 disabled:pointer-events-none shrink-0"
                      >
                        <Download className="w-3.5 h-3.5" />
                        Export CSV
                      </button>
                    </div>

                    <div className="grid grid-cols-2 gap-4">
                      <div className="bg-white p-5 rounded-2xl border border-slate-200/90 shadow-sm">
                        <p className="text-[0.6875rem] font-bold tracking-tight text-slate-500 uppercase mb-1">Total responses</p>
                        <div className="flex items-baseline gap-2 flex-wrap">
                          <span className="text-3xl font-extrabold text-slate-800 tracking-tight">{filteredFeedbackSummary.total}</span>
                          <span className="text-[10px] font-medium text-slate-400">
                            of {feedbackData.summary.total} all-time
                          </span>
                        </div>
                      </div>
                      <div className="bg-white p-5 rounded-2xl border border-slate-200/90 shadow-sm">
                        <p className="text-[0.6875rem] font-bold tracking-tight text-slate-500 uppercase mb-1">Aggregated NPS</p>
                        <div className="flex items-baseline gap-2 flex-wrap">
                          <span
                            className={clsx(
                              'text-3xl font-extrabold tracking-tight',
                              filteredFeedbackSummary.nps == null ? 'text-slate-400' : 'text-primary-700',
                            )}
                          >
                            {filteredFeedbackSummary.nps != null
                              ? `${filteredFeedbackSummary.nps > 0 ? '+' : ''}${filteredFeedbackSummary.nps}`
                              : '—'}
                          </span>
                          {npsQualityBadge && (
                            <span
                              className={clsx(
                                'px-2 py-0.5 text-[10px] rounded-full font-bold',
                                npsQualityBadge.className,
                              )}
                            >
                              {npsQualityBadge.label}
                            </span>
                          )}
                        </div>
                        <p className="text-[10px] text-slate-400 mt-1">Same formula as summary: (P − D) / (P + Pas + D) × 100</p>
                      </div>
                      <div className="bg-white p-5 rounded-2xl border border-slate-200/90 shadow-sm">
                        <p className="text-[0.6875rem] font-bold tracking-tight text-slate-500 uppercase mb-1">CSAT (Freshdesk)</p>
                        <div className="flex items-baseline gap-2">
                          {filteredFeedbackSummary.freshdesk_csat_avg != null ? (
                            <>
                              <span className="text-3xl font-extrabold text-slate-800 tracking-tight">
                                {filteredFeedbackSummary.freshdesk_csat_avg.toFixed(1)}
                              </span>
                              <span className="text-sm text-slate-500 font-medium">
                                / {filteredFeedbackSummary.freshdesk_csat_scale}.0
                              </span>
                            </>
                          ) : (
                            <span className="text-lg font-semibold text-slate-400">—</span>
                          )}
                        </div>
                        <p className="text-[10px] text-slate-400 mt-1">Avg. raw score in filtered Freshdesk rows</p>
                      </div>
                      <div className="bg-white p-5 rounded-2xl border border-slate-200/90 shadow-sm">
                        <p className="text-[0.6875rem] font-bold tracking-tight text-slate-500 uppercase mb-1">Unique accounts</p>
                        <div className="flex items-baseline gap-2">
                          <span className="text-3xl font-extrabold text-slate-800 tracking-tight">
                            {filteredFeedbackSummary.unique_customers}
                          </span>
                        </div>
                        <p className="text-[10px] text-slate-400 mt-1">
                          SM {filteredFeedbackSummary.survey_monkey_count} · FD {filteredFeedbackSummary.freshdesk_count}
                        </p>
                      </div>
                    </div>

                    <section>
                      <div className="flex justify-between items-end mb-3 px-0.5">
                        <h3 className="text-base font-bold text-slate-800">By customer</h3>
                      </div>
                      <div className="bg-white rounded-2xl border border-slate-200/90 overflow-hidden shadow-sm overflow-x-auto">
                        <table className="w-full text-left min-w-[520px]">
                          <thead>
                            <tr className="bg-slate-50/90">
                              <th className="py-3 px-4 text-[10px] font-bold text-slate-500 uppercase tracking-wider">Account</th>
                              <th className="py-3 px-2 text-[10px] font-bold text-slate-500 uppercase tracking-wider w-12">N</th>
                              <th className="py-3 px-4 text-[10px] font-bold text-slate-500 uppercase tracking-wider text-right">Composition</th>
                            </tr>
                          </thead>
                          <tbody className="divide-y divide-slate-100">
                            {filteredByCustomer.length === 0 ? (
                              <tr>
                                <td colSpan={3} className="px-4 py-10 text-center text-slate-400 text-sm">
                                  No rows match the current filters.
                                </td>
                              </tr>
                            ) : (
                              filteredByCustomer.map((row) => {
                                const denom = row.promoters + row.passives + row.detractors || row.count || 1
                                const pw = denom > 0 ? (row.promoters / denom) * 100 : 0
                                const paw = denom > 0 ? (row.passives / denom) * 100 : 0
                                const dw = denom > 0 ? (row.detractors / denom) * 100 : 0
                                return (
                                  <tr key={row.customer_name} className="hover:bg-slate-50/60 transition-colors">
                                    <td className="py-4 px-4 align-top">
                                      <p className="text-xs font-bold text-slate-800">{row.customer_name}</p>
                                      <p className="text-[10px] text-slate-500 mt-0.5">{row.region || '—'}</p>
                                    </td>
                                    <td className="py-4 px-2 text-xs font-medium text-slate-700 tabular-nums align-top">{row.count}</td>
                                    <td className="py-4 px-4 align-top">
                                      <div className="flex h-2 w-36 ml-auto rounded-full overflow-hidden bg-slate-200">
                                        {pw > 0 && (
                                          <div className="bg-emerald-500 h-full shrink-0" style={{ width: `${pw}%` }} title="Promoters" />
                                        )}
                                        {paw > 0 && (
                                          <div className="bg-amber-400 h-full shrink-0" style={{ width: `${paw}%` }} title="Passives" />
                                        )}
                                        {dw > 0 && (
                                          <div className="bg-rose-500 h-full shrink-0" style={{ width: `${dw}%` }} title="Detractors" />
                                        )}
                                      </div>
                                      <p className="text-[9px] text-right mt-1 text-slate-400">
                                        Last: {row.last_response_date ? formatDate(row.last_response_date) : '—'}
                                      </p>
                                    </td>
                                  </tr>
                                )
                              })
                            )}
                          </tbody>
                        </table>
                      </div>
                    </section>

                    <section className="space-y-4">
                      <div className="flex justify-between items-end px-0.5">
                        <h3 className="text-base font-bold text-slate-800">Recent feedback</h3>
                        <div className="flex items-center gap-1 text-slate-400">
                          <MessageCircle className="w-3.5 h-3.5" />
                          <span className="text-[10px] font-bold uppercase tracking-wide">Verbatim</span>
                        </div>
                      </div>
                      {recentVerbatimRows.length === 0 ? (
                        <p className="text-sm text-slate-400 px-1">
                          No recent feedback in the current filter. Try widening date/source filters or use the full response log below.
                        </p>
                      ) : (
                        <div className="space-y-4">
                          {recentVerbatimRows.map(({ r, text }, vidx) => (
                            <FeedbackVerbatimCard
                              key={`verbatim-${vidx}-${r.source}-${r.record_id}-${r.ticket_id}-${r.response_date ?? ''}`}
                              r={r}
                              previewText={text}
                            />
                          ))}
                        </div>
                      )}
                    </section>

                    <details className="group rounded-2xl border border-slate-200/90 bg-slate-50/50 overflow-hidden">
                      <summary className="cursor-pointer list-none px-4 py-3 text-sm font-semibold text-slate-700 hover:bg-slate-100/80 flex items-center justify-between">
                        <span>Full response log</span>
                        <span className="text-xs font-normal text-slate-400">{filteredFeedbackResponses.length} rows · newest first</span>
                      </summary>
                      <div className="px-2 pb-4">
                      <div className="bg-white rounded-xl border border-slate-200 overflow-hidden overflow-x-auto max-h-[min(420px,50vh)] overflow-y-auto mx-2">
                        <table className="w-full text-sm">
                          <thead className="sticky top-0 bg-slate-50 z-10">
                            <tr className="text-left text-[10px] uppercase text-slate-500 border-b border-slate-100">
                              <th className="w-8 px-1 py-2" aria-label="Details" />
                              <th className="px-3 py-2 font-semibold">Date</th>
                              <th className="px-3 py-2 font-semibold">Source</th>
                              <th className="px-3 py-2 font-semibold">Account</th>
                              <th className="px-3 py-2 font-semibold">Rating</th>
                              <th className="px-3 py-2 font-semibold">NPS</th>
                              <th className="px-3 py-2 font-semibold">AE</th>
                              <th className="px-3 py-2 font-semibold max-w-[140px]">Feedback</th>
                              <th className="px-3 py-2 font-semibold text-right">Link</th>
                            </tr>
                          </thead>
                          <tbody className="divide-y divide-slate-50">
                            {filteredFeedbackResponses.length === 0 ? (
                              <tr>
                                <td colSpan={9} className="px-3 py-8 text-center text-slate-400 text-sm">
                                  No rows.
                                </td>
                              </tr>
                            ) : (
                              filteredFeedbackResponses.map((r, idx) => {
                                const rowKey = `${r.source}-${r.record_id}-${r.ticket_id}-${idx}`
                                const expanded = expandedFeedbackKeys.has(rowKey)
                                const isSm = String(r.source || '').includes('Survey')
                                const isFd = String(r.source || '').includes('Freshdesk')
                                const canExpand = isSm || isFd
                                return (
                                  <Fragment key={rowKey}>
                                    <tr className="hover:bg-slate-50/80 align-top">
                                      <td className="px-1 py-2 align-top">
                                        {canExpand ? (
                                          <button
                                            type="button"
                                            className="p-1 rounded text-slate-400 hover:text-slate-700 hover:bg-slate-100"
                                            aria-expanded={expanded}
                                            title={expanded ? 'Hide details' : 'Show full response'}
                                            onClick={() => toggleFeedbackRow(rowKey)}
                                          >
                                            {expanded ? (
                                              <ChevronDown className="w-4 h-4" />
                                            ) : (
                                              <ChevronRight className="w-4 h-4" />
                                            )}
                                          </button>
                                        ) : (
                                          <span className="inline-block w-6" />
                                        )}
                                      </td>
                                      <td className="px-3 py-2 text-xs text-slate-600 whitespace-nowrap">
                                        {r.response_date ? formatDate(r.response_date) : '—'}
                                      </td>
                                      <td className="px-3 py-2 text-xs text-slate-600">{r.source}</td>
                                      <td className="px-3 py-2 text-xs font-medium text-slate-800 max-w-[160px]" title={r.customer_name || ''}>
                                        <span className="line-clamp-2">{r.customer_name || '—'}</span>
                                      </td>
                                      <td className="px-3 py-2 text-xs text-slate-600 max-w-[120px]">{r.rating_label || '—'}</td>
                                      <td className="px-3 py-2 text-xs">
                                        {r.nps_category ? (
                                          <span className={clsx(
                                            'font-semibold',
                                            r.nps_category === 'Promoter' && 'text-emerald-600',
                                            r.nps_category === 'Passive' && 'text-amber-600',
                                            r.nps_category === 'Detractor' && 'text-rose-600'
                                          )}>{r.nps_category}</span>
                                        ) : '—'}
                                      </td>
                                      <td className="px-3 py-2 text-xs text-slate-500 max-w-[120px]" title={r.ae || ''}>
                                        <span className="line-clamp-2">{r.ae || '—'}</span>
                                      </td>
                                      <td className="px-3 py-2 text-xs text-slate-600 max-w-[140px]" title={r.feedback || ''}>
                                        <span className="line-clamp-2 break-words">{feedbackPreview(r.feedback, 80)}</span>
                                      </td>
                                      <td className="px-3 py-2 text-right whitespace-nowrap">
                                        {r.drill_url ? (
                                          <a
                                            href={r.drill_url}
                                            target="_blank"
                                            rel="noopener noreferrer"
                                            className="inline-flex items-center gap-1 text-primary-600 hover:text-primary-700 text-xs font-medium"
                                          >
                                            {r.drill_label || 'Open'}
                                            <ExternalLink className="w-3 h-3" />
                                          </a>
                                        ) : (
                                          <span className="text-slate-300 text-xs">—</span>
                                        )}
                                      </td>
                                    </tr>
                                    {expanded && canExpand && (
                                      <tr className="bg-slate-50/95 border-b border-slate-100">
                                        <td colSpan={9} className="px-3 py-4 text-xs text-slate-700">
                                          <div className="pl-2 border-l-2 border-primary-400 space-y-4">
                                            {isSm && (
                                              <div>
                                                <p className="text-[10px] font-semibold text-slate-500 uppercase tracking-wide mb-2">
                                                  SurveyMonkey — all questions
                                                </p>
                                                {r.survey_questions && r.survey_questions.length > 0 ? (
                                                  <div className="space-y-3">
                                                    {r.survey_questions.map((qa, qidx) => (
                                                      <div key={qidx} className="pb-3 border-b border-slate-200/80 last:border-0 last:pb-0">
                                                        <p className="text-[11px] font-semibold text-slate-600">{qa.question || 'Question'}</p>
                                                        <p className="text-sm text-slate-800 mt-1 whitespace-pre-wrap break-words">
                                                          {qa.answer || '—'}
                                                        </p>
                                                      </div>
                                                    ))}
                                                  </div>
                                                ) : (
                                                  <p className="text-slate-400">No per-question answers returned.</p>
                                                )}
                                              </div>
                                            )}
                                            {isFd && (
                                              <div>
                                                <p className="text-[10px] font-semibold text-slate-500 uppercase tracking-wide mb-2">
                                                  Freshdesk — ticket &amp; CSAT
                                                </p>
                                                <div className="space-y-2 mb-3">
                                                  {r.ticket_id != null && (
                                                    <p>
                                                      <span className="font-semibold text-slate-600">Ticket</span>{' '}
                                                      <span className="tabular-nums font-medium text-slate-800">#{r.ticket_id}</span>
                                                    </p>
                                                  )}
                                                  {(r.ticket_status != null && String(r.ticket_status).trim()) && (
                                                    <p>
                                                      <span className="font-semibold text-slate-600">Status</span>{' '}
                                                      {String(r.ticket_status)}
                                                    </p>
                                                  )}
                                                  {(r.ticket_subject != null && r.ticket_subject.trim()) && (
                                                    <p>
                                                      <span className="font-semibold text-slate-600">Subject</span>{' '}
                                                      <span className="text-slate-800">{r.ticket_subject}</span>
                                                    </p>
                                                  )}
                                                </div>
                                                {r.csat_entries && r.csat_entries.length > 0 ? (
                                                  <div className="space-y-3">
                                                    {r.csat_entries.map((c, cidx) => (
                                                      <div key={cidx} className="rounded-lg bg-white border border-slate-200/80 p-3 space-y-1">
                                                        <div className="flex flex-wrap gap-x-3 gap-y-1 text-[11px]">
                                                          {c.label && (
                                                            <span><span className="text-slate-500">Prompt:</span> {c.label}</span>
                                                          )}
                                                          {c.rating_label && (
                                                            <span className="font-medium text-slate-800">{c.rating_label}</span>
                                                          )}
                                                          {c.nps_category && (
                                                            <span className={clsx(
                                                              'font-semibold',
                                                              c.nps_category === 'Promoter' && 'text-emerald-600',
                                                              c.nps_category === 'Passive' && 'text-amber-600',
                                                              c.nps_category === 'Detractor' && 'text-rose-600'
                                                            )}>{c.nps_category}</span>
                                                          )}
                                                          {c.response_date && (
                                                            <span className="text-slate-400">{formatDate(c.response_date)}</span>
                                                          )}
                                                        </div>
                                                        {c.feedback && c.feedback.trim() && (
                                                          <p className="text-sm text-slate-800 whitespace-pre-wrap break-words pt-1">
                                                            {c.feedback}
                                                          </p>
                                                        )}
                                                      </div>
                                                    ))}
                                                  </div>
                                                ) : (
                                                  (r.feedback != null && r.feedback.trim()) && (
                                                    <p>
                                                      <span className="font-semibold text-slate-600">CSAT feedback</span>
                                                      <span className="block mt-1 text-slate-800 whitespace-pre-wrap break-words">
                                                        {r.feedback}
                                                      </span>
                                                    </p>
                                                  )
                                                )}
                                              </div>
                                            )}
                                          </div>
                                        </td>
                                      </tr>
                                    )}
                                  </Fragment>
                                )
                              })
                            )}
                          </tbody>
                        </table>
                      </div>
                      </div>
                    </details>
                  </>
                )}
              </div>
            )
          )}
        </div>

        {/* Footer */}
        <div className="px-6 py-3 border-t border-slate-200 bg-slate-50 flex justify-end">
          <button onClick={onClose}
            className="px-4 py-2 text-sm font-medium text-slate-600 hover:text-slate-800 hover:bg-slate-200 rounded-lg transition-colors">
            Close
          </button>
        </div>
      </div>
    </>
  )
}

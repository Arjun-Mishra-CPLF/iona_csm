import { useState, useMemo, useEffect, useCallback, useRef } from 'react'
import { 
  Users, 
  UserPlus, 
  BarChart3, 
  ChevronRight,
  ChevronDown,
  AlertTriangle,
  CheckCircle,
  XCircle,
  Loader2,
  X,
  Clock,
  UserX,
  User,
  Info,
} from 'lucide-react'
import { clsx } from 'clsx'
import { useCSMStats, useCSMs, useAccountsWithCSM } from '../hooks/useCSM'
import { CSMProfilePanel } from '../components/CSMProfilePanel'
import { healthBadgeLabel } from '../utils/healthLabels'
import type { CSM, AccountWithCSM, CSMStatus, RenewalInfo } from '../types'

// Utility to format currency
function formatCurrency(value: number): string {
  if (value >= 1_000_000) return `€${(value / 1_000_000).toFixed(1)}M`
  if (value >= 1_000) return `€${(value / 1_000).toFixed(0)}K`
  return `€${value.toFixed(0)}`
}

// Health badge component
function HealthBadge({ health }: { health: string }) {
  const config = {
    Critical: { bg: 'bg-rose-100', text: 'text-rose-700', icon: XCircle },
    'At Risk': { bg: 'bg-amber-100', text: 'text-amber-700', icon: AlertTriangle },
    Good: { bg: 'bg-emerald-100', text: 'text-emerald-700', icon: CheckCircle },
    Unknown: { bg: 'bg-slate-100', text: 'text-slate-500', icon: null },
  }[health] || { bg: 'bg-slate-100', text: 'text-slate-500', icon: null }

  const Icon = config.icon

  return (
    <span className={clsx('inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium whitespace-nowrap', config.bg, config.text)}>
      {Icon && <Icon className="w-3 h-3" />}
      {healthBadgeLabel(health)}
    </span>
  )
}

// CSM Status badge component
function CSMStatusBadge({ status }: { status: CSMStatus }) {
  const config = {
    active: { bg: 'bg-emerald-100', text: 'text-emerald-700', label: 'Active', icon: CheckCircle },
    inactive: { bg: 'bg-slate-100', text: 'text-slate-600', label: 'Inactive', icon: Clock },
    departed: { bg: 'bg-rose-100', text: 'text-rose-700', label: 'Departed', icon: UserX },
  }[status]

  const Icon = config.icon

  return (
    <span className={clsx('inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium', config.bg, config.text)}>
      <Icon className="w-3 h-3" />
      {config.label}
    </span>
  )
}

// CSM employment type
type CSMType = 'full-time' | 'part-time' | 'backfill'
const CSM_TYPE_OPTIONS: { value: CSMType; label: string; short: string; bg: string; text: string }[] = [
  { value: 'full-time', label: 'Full-Time', short: 'FT', bg: 'bg-emerald-100', text: 'text-emerald-700' },
  { value: 'part-time', label: 'Part-Time', short: 'PT', bg: 'bg-blue-100', text: 'text-blue-700' },
  { value: 'backfill', label: 'Backfill', short: 'BF', bg: 'bg-amber-100', text: 'text-amber-700' },
]

function CSMTypeBadge({
  csmId,
  currentType,
  onTypeChange,
}: {
  csmId: string
  currentType: CSMType
  onTypeChange: (csmId: string, type: CSMType) => void
}) {
  const [open, setOpen] = useState(false)
  const [openUp, setOpenUp] = useState(false)
  const btnRef = useRef<HTMLButtonElement>(null)
  const current = CSM_TYPE_OPTIONS.find(o => o.value === currentType) ?? CSM_TYPE_OPTIONS[0]

  const handleOpen = (e: React.MouseEvent) => {
    e.stopPropagation()
    if (!open && btnRef.current) {
      const rect = btnRef.current.getBoundingClientRect()
      const spaceBelow = window.innerHeight - rect.bottom
      setOpenUp(spaceBelow < 120)
    }
    setOpen(!open)
  }

  return (
    <div className="relative">
      <button
        ref={btnRef}
        onClick={handleOpen}
        className={clsx(
          'inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-semibold transition-colors',
          current.bg, current.text
        )}
        title="Change CSM type"
      >
        {current.short}
      </button>
      {open && (
        <>
          <div className="fixed inset-0 z-20" onClick={(e) => { e.stopPropagation(); setOpen(false) }} />
          <div className={clsx(
            'absolute left-0 z-30 bg-white border border-slate-200 rounded-lg shadow-lg w-[130px] py-1 overflow-hidden',
            openUp ? 'bottom-full mb-1' : 'top-full mt-1'
          )}>
            {CSM_TYPE_OPTIONS.map(opt => (
              <button
                key={opt.value}
                onClick={(e) => {
                  e.stopPropagation()
                  onTypeChange(csmId, opt.value)
                  setOpen(false)
                }}
                className={clsx(
                  'w-full text-left px-3 py-1.5 text-xs hover:bg-slate-50 transition-colors flex items-center gap-2',
                  currentType === opt.value ? 'font-bold text-primary-600 bg-primary-50/50' : 'text-slate-600'
                )}
              >
                <span className={clsx('w-2 h-2 rounded-full', opt.bg.replace('100', '500'))} />
                {opt.label}
              </button>
            ))}
          </div>
        </>
      )}
    </div>
  )
}

// Renewal cell with tooltip (same pattern as homepage)
function RenewalCell({ renewals, fallbackDays }: { renewals?: RenewalInfo[]; fallbackDays: number | null }) {
  const [showTooltip, setShowTooltip] = useState(false)

  if (!renewals || renewals.length === 0) {
    if (fallbackDays === null || fallbackDays === undefined) return <span className="text-sm text-slate-400">-</span>
    if (fallbackDays >= 999) return <span className="text-sm text-slate-400">—</span>
    return <span className="text-sm text-slate-600">{fallbackDays}d</span>
  }

  const sorted = [...renewals].sort((a, b) => (a.renewal_days ?? 9999) - (b.renewal_days ?? 9999))
  const primary = sorted[0]
  const hasMultiple = sorted.length > 1

  const formatDate = (d: string | null) => {
    if (!d) return '—'
    try {
      return new Date(d).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
    } catch { return d }
  }

  const typeBadgeColor = (t: string) =>
    t === 'SaaS' ? 'bg-blue-100 text-blue-700' : 'bg-purple-100 text-purple-700'

  const daysColor = (days: number | null) =>
    days !== null && days <= 30 ? 'text-rose-600' :
    days !== null && days <= 90 ? 'text-amber-600' : 'text-slate-700'

  return (
    <div
      className="relative inline-flex"
      onMouseEnter={() => setShowTooltip(true)}
      onMouseLeave={() => setShowTooltip(false)}
    >
      <div className="flex items-center gap-1.5">
        <span className={clsx('text-sm font-medium', daysColor(primary.renewal_days))}>
          {primary.renewal_days != null ? `${primary.renewal_days}d` : '—'}
        </span>
        <span className={clsx('inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-semibold', typeBadgeColor(primary.revenue_type))}>
          {primary.revenue_type}
        </span>
        {hasMultiple && (
          <span className="inline-flex items-center justify-center w-4 h-4 rounded-full bg-slate-200 text-[9px] font-bold text-slate-600">
            +{sorted.length - 1}
          </span>
        )}
      </div>

      {showTooltip && (
        <div className="absolute left-0 top-full mt-2 z-50 bg-slate-800 text-white rounded-lg shadow-lg px-3 py-2.5 min-w-[200px]">
          <div className="absolute left-4 -top-1 w-2 h-2 bg-slate-800 rotate-45" />
          <div className="text-[10px] uppercase tracking-wider text-slate-400 mb-1.5">Renewal Dates</div>
          <div className="space-y-1.5">
            {sorted.map((r) => (
              <div key={r.revenue_type} className="flex items-center justify-between gap-4">
                <span className={clsx('inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-semibold',
                  r.revenue_type === 'SaaS' ? 'bg-blue-500/20 text-blue-300' : 'bg-purple-500/20 text-purple-300'
                )}>
                  {r.revenue_type}
                </span>
                <div className="text-right">
                  <div className="text-xs font-medium">{formatDate(r.renewal_date)}</div>
                  <div className="text-[10px] text-slate-400">
                    {r.renewal_days != null ? `${r.renewal_days} days` : '—'}
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

// FTE capacity weight per CSM type
const FTE_WEIGHT: Record<CSMType, number> = {
  'full-time': 1.0,
  'part-time': 0.5,
  'backfill': 0.5,
}

// Rich stat card matching homepage Companies card style
function ActiveCSMsCard({
  total,
  typeCounts,
  isLoading,
}: {
  total: number
  typeCounts: { ft: number; pt: number; bf: number }
  isLoading?: boolean
}) {
  const segments = [
    { count: typeCounts.ft, color: 'bg-emerald-500', label: 'Full-Time', dot: 'bg-emerald-500' },
    { count: typeCounts.pt, color: 'bg-blue-500', label: 'Part-Time', dot: 'bg-blue-500' },
    { count: typeCounts.bf, color: 'bg-amber-500', label: 'Backfill', dot: 'bg-amber-500' },
  ].filter(s => s.count > 0)

  return (
    <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
      <div className="flex items-center justify-between mb-3">
        <p className="text-xs font-semibold text-slate-500 uppercase tracking-wider">Active CSMs</p>
        <div className={clsx('w-9 h-9 rounded-xl flex items-center justify-center bg-primary-100')}>
          <Users className="w-4 h-4 text-primary-600" />
        </div>
      </div>
      {isLoading ? (
        <div className="h-8 w-16 bg-slate-200 rounded animate-pulse mb-3" />
      ) : (
        <p className="text-3xl font-bold text-slate-800 mb-3">{total}</p>
      )}
      {!isLoading && total > 0 && (
        <>
          <div className="flex h-2 rounded-full overflow-hidden bg-slate-100 mb-2">
            {segments.map(s => (
              <div
                key={s.label}
                className={clsx('h-full transition-all', s.color)}
                style={{ width: `${(s.count / total) * 100}%` }}
              />
            ))}
          </div>
          <div className="flex items-center gap-3 flex-wrap">
            {segments.map(s => (
              <span key={s.label} className="flex items-center gap-1 text-[11px] text-slate-600">
                <span className={clsx('w-2 h-2 rounded-full', s.dot)} />
                {s.count} {s.label}
              </span>
            ))}
          </div>
        </>
      )}
    </div>
  )
}

function AvgAccountsCard({
  ftAvg,
  ptAvg,
  fteAvg,
  isLoading,
}: {
  ftAvg: number
  ptAvg: number
  fteAvg: number
  isLoading?: boolean
}) {
  return (
    <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
      <div className="flex items-center justify-between mb-3">
        <p className="text-xs font-semibold text-slate-500 uppercase tracking-wider">Avg Accounts / CSM</p>
        <div className="w-9 h-9 rounded-xl flex items-center justify-center bg-emerald-100">
          <BarChart3 className="w-4 h-4 text-emerald-600" />
        </div>
      </div>
      {isLoading ? (
        <div className="h-8 w-16 bg-slate-200 rounded animate-pulse mb-3" />
      ) : (
        <p className="text-3xl font-bold text-slate-800 mb-3">{fteAvg.toFixed(1)} <span className="text-sm font-normal text-slate-400">per FTE</span></p>
      )}
      {!isLoading && (
        <div className="flex items-center gap-4">
          <span className="flex items-center gap-1 text-[11px] text-slate-600">
            <span className="w-2 h-2 rounded-full bg-emerald-500" />
            {ftAvg.toFixed(1)} FT avg
          </span>
          {ptAvg > 0 && (
            <span className="flex items-center gap-1 text-[11px] text-slate-600">
              <span className="w-2 h-2 rounded-full bg-blue-500" />
              {ptAvg.toFixed(1)} PT avg
            </span>
          )}
        </div>
      )}
    </div>
  )
}

function UnassignedCard({
  value,
  isLoading,
  isActive,
  onClick,
}: {
  value: number | string
  isLoading?: boolean
  isActive?: boolean
  onClick?: () => void
}) {
  const count = typeof value === 'number' ? value : 0
  const hasUnassigned = count > 0
  return (
    <div
      className={clsx(
        'bg-white rounded-xl shadow-sm border p-5 transition-all',
        hasUnassigned && 'cursor-pointer hover:shadow-md',
        isActive
          ? 'border-amber-400 ring-2 ring-amber-100 bg-amber-50/30'
          : hasUnassigned
            ? 'border-amber-200 hover:border-amber-300 bg-amber-50/20'
            : 'border-emerald-200 bg-emerald-50/20'
      )}
      onClick={hasUnassigned ? onClick : undefined}
    >
      <div className="flex items-center justify-between mb-3">
        <p className="text-xs font-semibold text-slate-500 uppercase tracking-wider">Unassigned Accounts</p>
        <div className={clsx(
          'w-9 h-9 rounded-xl flex items-center justify-center',
          hasUnassigned ? 'bg-amber-100' : 'bg-emerald-100'
        )}>
          <UserPlus className={clsx('w-4 h-4', hasUnassigned ? 'text-amber-600' : 'text-emerald-600')} />
        </div>
      </div>
      {isLoading ? (
        <div className="h-8 w-16 bg-slate-200 rounded animate-pulse" />
      ) : (
        <p className={clsx(
          'text-3xl font-bold',
          hasUnassigned ? 'text-amber-700' : 'text-emerald-700'
        )}>{value}</p>
      )}
      {hasUnassigned ? (
        <p className="text-[11px] text-amber-500 mt-1">Click to view</p>
      ) : (
        <p className="text-[11px] text-emerald-500 mt-1">All accounts assigned</p>
      )}
    </div>
  )
}

// CSM Row with expandable accounts
function CSMRow({ 
  csm, 
  isExpanded, 
  onToggle,
  onViewProfile,
  expectedAccounts,
  csmType,
  onTypeChange,
  accountType,
}: { 
  csm: CSM
  isExpanded: boolean
  onToggle: () => void
  onViewProfile: () => void
  expectedAccounts: number
  csmType: CSMType
  onTypeChange: (csmId: string, type: CSMType) => void
  accountType?: string
}) {
  // Fetch accounts for this CSM when expanded
  const { data: accountsData, isLoading: accountsLoading } = useAccountsWithCSM(
    {
      csm_id: csm.id,
      page_size: 100,
      account_type: accountType,
    },
    { enabled: isExpanded }
  )

  const workloadPercent = expectedAccounts > 0 ? (csm.account_count / expectedAccounts) * 100 : 100
  const workloadLevel = workloadPercent > 120 ? 'high' : workloadPercent < 80 ? 'low' : 'normal'

  const isInactive = csm.status === 'inactive'
  const isDeparted = csm.status === 'departed'

  return (
    <>
      {/* CSM Main Row */}
      <tr 
        className={clsx(
          'transition-colors cursor-pointer',
          isExpanded && 'bg-slate-50',
          isDeparted ? 'bg-rose-50/30 hover:bg-rose-50/50' :
          isInactive ? 'bg-slate-50/50 hover:bg-slate-100/50' :
          'hover:bg-slate-50'
        )}
        onClick={onToggle}
      >
        <td className="py-3 px-4">
          <button className="p-1 text-slate-400 hover:text-slate-600">
            {isExpanded ? (
              <ChevronDown className="w-4 h-4" />
            ) : (
              <ChevronRight className="w-4 h-4" />
            )}
          </button>
        </td>
        <td className="py-3 px-4">
          <div className="flex items-center gap-3">
            <div className={clsx(
              'w-9 h-9 rounded-full flex items-center justify-center text-white text-sm font-semibold flex-shrink-0',
              isDeparted ? 'bg-gradient-to-br from-slate-400 to-slate-500' :
              isInactive ? 'bg-gradient-to-br from-slate-300 to-slate-400' :
              'bg-gradient-to-br from-primary-400 to-primary-600'
            )}>
              {csm.name.split(' ').map(n => n[0]).join('')}
            </div>
            <div className="flex items-center gap-2 min-w-0 flex-1">
              <button
                type="button"
                onClick={(e) => { e.stopPropagation(); onViewProfile(); }}
                className="flex-shrink-0 flex items-center justify-center w-8 h-8 rounded-lg border-2 border-primary-300 bg-primary-50 text-primary-600 hover:bg-primary-100 hover:border-primary-400 shadow-sm transition-colors"
                title="View CSM profile"
                aria-label="View CSM profile"
              >
                <User className="w-4 h-4" />
              </button>
              <div className="flex items-center gap-1.5 min-w-0 flex-wrap">
                <p className={clsx(
                  'font-medium truncate',
                  isDeparted ? 'text-slate-500 line-through' :
                  isInactive ? 'text-slate-600' :
                  'text-slate-800'
                )}>
                  {csm.name}
                </p>
                {(isInactive || isDeparted) && (
                  <CSMStatusBadge status={csm.status} />
                )}
              </div>
            </div>
          </div>
        </td>
        <td className="py-3 px-4 text-center">
          <CSMTypeBadge csmId={csm.id} currentType={csmType} onTypeChange={onTypeChange} />
        </td>
        <td className="py-3 px-4 text-center">
          <span className={clsx(
            'text-sm font-semibold',
            isDeparted ? 'text-rose-600' : 'text-slate-800'
          )}>
            {csm.account_count}
            {isDeparted && csm.account_count > 0 && (
              <span className="ml-1 text-xs font-normal text-rose-500">(reassign)</span>
            )}
          </span>
        </td>
        <td className="py-3 px-4 text-right">
          <span className={clsx(
            'text-sm font-medium',
            isDeparted ? 'text-slate-500' : 'text-slate-800'
          )}>
            {formatCurrency(csm.total_arr)}
          </span>
        </td>
        <td className="py-3 px-4 text-right">
          <span className="text-sm text-slate-400">—</span>
        </td>
        <td className="py-3 px-4 text-center">
          {csm.at_risk_count > 0 ? (
            <span className="inline-flex items-center gap-1 px-2 py-0.5 bg-rose-100 text-rose-700 rounded-full text-xs font-medium">
              <AlertTriangle className="w-3 h-3" />
              {csm.at_risk_count}
            </span>
          ) : (
            <span className="inline-flex items-center gap-1 px-2 py-0.5 bg-emerald-100 text-emerald-700 rounded-full text-xs font-medium">
              <CheckCircle className="w-3 h-3" />
              0
            </span>
          )}
        </td>
        <td className="py-3 px-4">
          {isDeparted ? (
            <span className="text-xs text-rose-600 font-medium">N/A</span>
          ) : (
            <div className="flex items-center justify-center gap-2">
              <div className="w-20 h-2 bg-slate-100 rounded-full overflow-hidden">
                <div 
                  className={clsx(
                    'h-full rounded-full transition-all',
                    workloadLevel === 'high' ? 'bg-rose-500' :
                    workloadLevel === 'low' ? 'bg-amber-500' :
                    'bg-emerald-500'
                  )}
                  style={{ width: `${Math.min(workloadPercent, 100)}%` }}
                />
              </div>
              <span className={clsx(
                'text-xs font-medium w-10',
                workloadLevel === 'high' ? 'text-rose-600' :
                workloadLevel === 'low' ? 'text-amber-600' :
                'text-emerald-600'
              )}>
                {workloadPercent.toFixed(0)}%
              </span>
            </div>
          )}
        </td>
      </tr>

      {/* Expanded Accounts */}
      {isExpanded && (
        <tr>
          <td colSpan={9} className="p-0">
            <div className={clsx(
              'border-t',
              isDeparted ? 'bg-rose-50/30' : 'bg-gradient-to-b from-slate-50/80 to-white'
            )}>
              {accountsLoading ? (
                <div className="flex items-center justify-center py-8">
                  <Loader2 className="w-5 h-5 text-primary-500 animate-spin" />
                  <span className="ml-2 text-sm text-slate-500">Loading accounts...</span>
                </div>
              ) : accountsData?.accounts && accountsData.accounts.length > 0 ? (
                <div className="px-6 py-4">
                  {isDeparted && (
                    <div className="mb-3 px-4 py-2.5 bg-rose-100/80 rounded-lg text-sm text-rose-700 flex items-center gap-2">
                      <AlertTriangle className="w-4 h-4 flex-shrink-0" />
                      These accounts need to be reassigned to an active CSM
                    </div>
                  )}
                  {/* Summary strip */}
                  <div className="flex items-center gap-4 mb-3 px-1">
                    <span className="text-xs font-medium text-slate-500">
                      {accountsData.accounts.length} account{accountsData.accounts.length !== 1 ? 's' : ''}
                    </span>
                    <div className="flex-1 h-px bg-slate-200" />
                    {(() => {
                      const atRisk = accountsData.accounts.filter(a => a.health === 'At Risk' || a.health === 'Critical').length
                      return atRisk > 0 ? (
                        <span className="text-xs font-medium text-amber-600 flex items-center gap-1">
                          <AlertTriangle className="w-3 h-3" /> {atRisk} needs attention
                        </span>
                      ) : null
                    })()}
                  </div>
                  {/* Account cards grid */}
                  <div className="grid gap-2">
                    {accountsData.accounts.map((account) => (
                      <div
                        key={account.id}
                        className="group flex items-center gap-4 px-4 py-3 rounded-lg bg-white border border-slate-100 hover:border-slate-200 hover:shadow-sm transition-all cursor-pointer"
                      >
                        {/* Account name + avatar */}
                        <div className="flex items-center gap-3 min-w-0 flex-1">
                          <div className={clsx(
                            'w-8 h-8 rounded-lg flex items-center justify-center text-xs font-bold flex-shrink-0',
                            account.health === 'Critical' ? 'bg-rose-100 text-rose-700' :
                            account.health === 'At Risk' ? 'bg-amber-100 text-amber-700' :
                            'bg-primary-50 text-primary-600'
                          )}>
                            {account.name.charAt(0)}
                          </div>
                          <div className="min-w-0">
                            <p className="text-sm font-medium text-slate-800 truncate group-hover:text-primary-700 transition-colors">
                              {account.name}
                            </p>
                            <p className="text-[11px] text-slate-400">{account.account_type || 'Unknown'}</p>
                          </div>
                        </div>

                        {/* ARR */}
                        <div className="w-24 text-right flex-shrink-0">
                          <p className="text-sm font-semibold text-slate-800">{formatCurrency(account.arr)}</p>
                          <p className="text-[11px] text-slate-400">ARR</p>
                        </div>

                        {/* Health */}
                        <div className="w-20 text-center flex-shrink-0">
                          <HealthBadge health={account.health} />
                        </div>

                        {/* Renewal */}
                        <div className="w-28 text-right flex-shrink-0">
                          <RenewalCell renewals={account.renewals} fallbackDays={account.renewal_days} />
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              ) : (
                <div className="text-center py-8 text-slate-400 text-sm">
                  No accounts assigned
                </div>
              )}
            </div>
          </td>
        </tr>
      )}
    </>
  )
}

// Unassigned Accounts Section (as a table section)
function UnassignedAccountsTable({ 
  accounts, 
  isLoading 
}: { 
  accounts: AccountWithCSM[]
  isLoading: boolean 
}) {
  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-12">
        <Loader2 className="w-6 h-6 text-primary-500 animate-spin" />
      </div>
    )
  }

  if (accounts.length === 0) {
    return (
      <div className="text-center py-12 text-slate-500">
        <CheckCircle className="w-12 h-12 mx-auto mb-4 text-emerald-300" />
        <p className="text-lg font-medium text-emerald-700">All accounts assigned</p>
        <p className="text-sm mt-1">No unassigned accounts at this time</p>
      </div>
    )
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full">
        <thead className="bg-amber-50 sticky top-0">
          <tr className="border-b border-amber-200">
            <th className="text-left py-3 px-4 text-xs font-semibold text-amber-700 uppercase tracking-wider">Account</th>
            <th className="text-left py-3 px-4 text-xs font-semibold text-amber-700 uppercase tracking-wider">Type</th>
            <th className="text-right py-3 px-4 text-xs font-semibold text-amber-700 uppercase tracking-wider">ARR</th>
            <th className="text-center py-3 px-4 text-xs font-semibold text-amber-700 uppercase tracking-wider">Health</th>
            <th className="text-center py-3 px-4 text-xs font-semibold text-amber-700 uppercase tracking-wider">Renewal</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-amber-100 bg-amber-50/30">
          {accounts.map((account) => (
            <tr key={account.id} className="hover:bg-amber-50 transition-colors">
              <td className="py-3 px-4">
                <div className="flex items-center gap-3">
                  <div className="w-8 h-8 rounded-full bg-amber-200 flex items-center justify-center text-amber-700 text-xs font-semibold">
                    {account.name.charAt(0)}
                  </div>
                  <span className="font-medium text-slate-800">{account.name}</span>
                </div>
              </td>
              <td className="py-3 px-4">
                <span className="text-sm text-slate-600">{account.account_type || '-'}</span>
              </td>
              <td className="py-3 px-4 text-right">
                <span className="text-sm font-medium text-slate-800">{formatCurrency(account.arr)}</span>
              </td>
              <td className="py-3 px-4 text-center">
                <HealthBadge health={account.health} />
              </td>
              <td className="py-3 px-4 text-center">
                <RenewalCell renewals={account.renewals} fallbackDays={account.renewal_days} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// Main component
export function ManageCSM({ accountTypeFilter = 'Customer' }: { accountTypeFilter?: string }) {
  const [expandedCSMs, setExpandedCSMs] = useState<Set<string>>(new Set())
  const [showUnassignedOnly, setShowUnassignedOnly] = useState(false)
  const [csmStatusFilter, setCsmStatusFilter] = useState<string>('')
  const [csmTypeFilter, setCsmTypeFilter] = useState<string>('')
  const [selectedCSM, setSelectedCSM] = useState<CSM | null>(null)

  // CSM type tagging (shared JSON file via /api/csm/types)
  const [csmTypes, setCsmTypes] = useState<Record<string, CSMType>>({})

  useEffect(() => {
    fetch('/api/csm/types')
      .then(r => r.json())
      .then(data => { if (data && typeof data === 'object') setCsmTypes(data) })
      .catch(() => {})
  }, [])

  const handleTypeChange = useCallback((csmId: string, type: CSMType) => {
    setCsmTypes(prev => ({ ...prev, [csmId]: type }))
    fetch('/api/csm/types', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ csm_id: csmId, csm_type: type }),
    }).catch(() => {})
  }, [])

  // CSM filter dropdown (like homepage)
  const [csmFilter, setCsmFilter] = useState<string | null>(null)
  const [csmDropdownOpen, setCsmDropdownOpen] = useState(false)
  const [csmSearch, setCsmSearch] = useState('')

  const acctParam = accountTypeFilter !== 'all' ? accountTypeFilter : undefined

  // Queries — all scoped by account type
  const { data: stats, isLoading: statsLoading } = useCSMStats({ account_type: acctParam })
  const { data: csmsData, isLoading: csmsLoading } = useCSMs({ status: csmStatusFilter || undefined, account_type: acctParam })
  const { data: allCSMsData } = useCSMs({ status: 'active', account_type: acctParam })
  const { data: unassignedData, isLoading: unassignedLoading } = useAccountsWithCSM({
    unassigned_only: true,
    page_size: 100,
    account_type: acctParam,
  })

  // Full CSM name list for the filter dropdown (independent of status filter)
  const allCSMNames = useMemo(() => {
    if (!allCSMsData?.csms) return []
    return allCSMsData.csms.map(c => c.name).sort()
  }, [allCSMsData])

  // Filter dropdown list by search input
  const dropdownCSMs = useMemo(() => {
    if (!csmSearch.trim()) return allCSMNames
    const q = csmSearch.toLowerCase()
    return allCSMNames.filter(name => name.toLowerCase().includes(q))
  }, [allCSMNames, csmSearch])

  // Filter CSMs by selected CSM name and type
  const filteredCSMs = useMemo(() => {
    if (!csmsData?.csms) return []
    let result = csmsData.csms
    if (csmFilter) result = result.filter(csm => csm.name === csmFilter)
    if (csmTypeFilter) {
      result = result.filter(csm => (csmTypes[csm.id] ?? 'full-time') === csmTypeFilter)
    }
    return result
  }, [csmsData, csmFilter, csmTypeFilter, csmTypes])

  // CSM type breakdown counts
  const typeCounts = useMemo(() => {
    if (!csmsData?.csms) return { ft: 0, pt: 0, bf: 0 }
    const active = csmsData.csms.filter(c => c.status === 'active')
    let ft = 0, pt = 0, bf = 0
    for (const c of active) {
      const t = csmTypes[c.id] ?? 'full-time'
      if (t === 'full-time') ft++
      else if (t === 'part-time') pt++
      else bf++
    }
    return { ft, pt, bf }
  }, [csmsData, csmTypes])

  // FTE-weighted workload: fair share per FTE unit
  const fairSharePerFTE = useMemo(() => {
    if (!csmsData?.csms) return 0
    const active = csmsData.csms.filter(c => c.status === 'active')
    if (active.length === 0) return 0
    const totalAccounts = active.reduce((sum, c) => sum + c.account_count, 0)
    const totalFTE = active.reduce((sum, c) => sum + FTE_WEIGHT[csmTypes[c.id] ?? 'full-time'], 0)
    return totalFTE > 0 ? totalAccounts / totalFTE : 0
  }, [csmsData, csmTypes])

  // Average accounts by type (for stat card)
  const avgByType = useMemo(() => {
    if (!csmsData?.csms) return { ft: 0, pt: 0 }
    const active = csmsData.csms.filter(c => c.status === 'active')
    let ftSum = 0, ftCount = 0, ptSum = 0, ptCount = 0
    for (const c of active) {
      const t = csmTypes[c.id] ?? 'full-time'
      if (t === 'full-time') { ftSum += c.account_count; ftCount++ }
      else { ptSum += c.account_count; ptCount++ }
    }
    return {
      ft: ftCount > 0 ? ftSum / ftCount : 0,
      pt: ptCount > 0 ? ptSum / ptCount : 0,
    }
  }, [csmsData, csmTypes])

  // Toggle CSM expansion
  const toggleCSM = (csmId: string) => {
    setExpandedCSMs(prev => {
      const next = new Set(prev)
      if (next.has(csmId)) {
        next.delete(csmId)
      } else {
        next.add(csmId)
      }
      return next
    })
  }

  // Handle clicking on Unassigned stat card
  const handleUnassignedClick = () => {
    setShowUnassignedOnly(!showUnassignedOnly)
  }

  // Clear all filters
  const clearFilters = () => {
    setCsmFilter(null)
    setCsmSearch('')
    setShowUnassignedOnly(false)
    setCsmStatusFilter('')
    setCsmTypeFilter('')
  }

  const hasActiveFilters = csmFilter || showUnassignedOnly || csmStatusFilter || csmTypeFilter

  return (
    <div className="p-6 bg-slate-50 min-h-full">
      {/* Header */}
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-slate-800">CSM Assignments</h1>
        <p className="text-slate-500 mt-1">Manage Customer Success Manager assignments and workload distribution</p>
      </div>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
        <ActiveCSMsCard
          total={stats?.active_csms ?? 0}
          typeCounts={typeCounts}
          isLoading={statsLoading || csmsLoading}
        />
        <AvgAccountsCard
          ftAvg={avgByType.ft}
          ptAvg={avgByType.pt}
          fteAvg={fairSharePerFTE}
          isLoading={statsLoading || csmsLoading}
        />
        <UnassignedCard
          value={stats?.unassigned_accounts ?? '-'}
          isLoading={statsLoading}
          isActive={showUnassignedOnly}
          onClick={handleUnassignedClick}
        />
      </div>

      {/* Main Content Card */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
        {/* Filter Bar */}
        <div className="px-4 py-3 border-b border-slate-200 flex flex-wrap items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <h2 className="text-lg font-semibold text-slate-800">
              {showUnassignedOnly ? 'Unassigned Accounts' : 'CSMs'}
            </h2>
            {!showUnassignedOnly && csmsData && (
              <span className="px-2 py-0.5 bg-slate-100 text-slate-600 text-xs font-medium rounded-full">
                {filteredCSMs.length}
              </span>
            )}
            {showUnassignedOnly && unassignedData && (
              <span className="px-2 py-0.5 bg-amber-100 text-amber-700 text-xs font-medium rounded-full">
                {unassignedData.accounts.length}
              </span>
            )}
          </div>

          <div className="flex items-center gap-3 flex-wrap">
            {/* CSM Filter Dropdown (combined search + select, like homepage) */}
            {!showUnassignedOnly && (
              <div className="relative">
                <button
                  onClick={() => { setCsmDropdownOpen(!csmDropdownOpen); setCsmSearch('') }}
                  className={`px-3 py-1.5 rounded-lg text-xs font-medium flex items-center gap-1.5 transition-colors ${
                    csmFilter
                      ? 'bg-primary-50 text-primary-700 border border-primary-200'
                      : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
                  }`}
                >
                  CSM: <span className="font-bold">{csmFilter || 'All'}</span>
                  {csmFilter ? (
                    <X
                      className="w-3 h-3 hover:text-primary-900"
                      onClick={(e) => { e.stopPropagation(); setCsmFilter(null); setCsmDropdownOpen(false) }}
                    />
                  ) : (
                    <ChevronDown className="w-3 h-3" />
                  )}
                </button>
                {csmDropdownOpen && (
                  <>
                    <div className="fixed inset-0 z-20" onClick={() => setCsmDropdownOpen(false)} />
                    <div className="absolute right-0 top-full mt-1 z-30 bg-white border border-slate-200 rounded-lg shadow-lg w-[220px] overflow-hidden">
                      <div className="p-2 border-b border-slate-100">
                        <input
                          type="text"
                          value={csmSearch}
                          onChange={(e) => setCsmSearch(e.target.value)}
                          placeholder="Search CSM..."
                          className="w-full px-2.5 py-1.5 text-xs border border-slate-200 rounded-md focus:outline-none focus:ring-1 focus:ring-primary-300 focus:border-primary-300 placeholder-slate-400"
                          autoFocus
                        />
                      </div>
                      <div className="max-h-[240px] overflow-y-auto py-1">
                        {!csmSearch.trim() && (
                          <button
                            onClick={() => { setCsmFilter(null); setCsmDropdownOpen(false) }}
                            className={`w-full text-left px-3 py-2 text-xs hover:bg-slate-50 transition-colors ${!csmFilter ? 'font-bold text-primary-600 bg-primary-50/50' : 'text-slate-600'}`}
                          >
                            All CSMs
                          </button>
                        )}
                        {dropdownCSMs.length === 0 ? (
                          <div className="px-3 py-3 text-xs text-slate-400 text-center">No CSMs found</div>
                        ) : (
                          dropdownCSMs.map(csm => (
                            <button
                              key={csm}
                              onClick={() => { setCsmFilter(csm); setCsmDropdownOpen(false); setCsmSearch('') }}
                              className={`w-full text-left px-3 py-2 text-xs hover:bg-slate-50 transition-colors ${csmFilter === csm ? 'font-bold text-primary-600 bg-primary-50/50' : 'text-slate-600'}`}
                            >
                              {csm}
                            </button>
                          ))
                        )}
                      </div>
                    </div>
                  </>
                )}
              </div>
            )}

            {/* CSM Status Filter */}
            {!showUnassignedOnly && (
              <select
                value={csmStatusFilter}
                onChange={(e) => setCsmStatusFilter(e.target.value)}
                className="px-3 py-1.5 text-xs border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent bg-white"
              >
                <option value="">All Status</option>
                <option value="active">Active</option>
                <option value="inactive">Inactive</option>
                <option value="departed">Departed</option>
              </select>
            )}

            {/* CSM Type Filter */}
            {!showUnassignedOnly && (
              <select
                value={csmTypeFilter}
                onChange={(e) => setCsmTypeFilter(e.target.value)}
                className="px-3 py-1.5 text-xs border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent bg-white"
              >
                <option value="">All Types</option>
                <option value="full-time">Full-Time</option>
                <option value="part-time">Part-Time</option>
                <option value="backfill">Backfill</option>
              </select>
            )}

            {/* Unassigned Toggle */}
            <label className="flex items-center gap-2 cursor-pointer px-3 py-1.5 rounded-lg hover:bg-slate-50 transition-colors">
              <input
                type="checkbox"
                checked={showUnassignedOnly}
                onChange={(e) => setShowUnassignedOnly(e.target.checked)}
                className="w-3.5 h-3.5 text-amber-600 border-slate-300 rounded focus:ring-amber-500"
              />
              <span className="text-xs text-slate-600">Unassigned only</span>
            </label>

            {/* Clear Filters */}
            {hasActiveFilters && (
              <button
                onClick={clearFilters}
                className="flex items-center gap-1.5 px-3 py-1.5 text-xs text-slate-500 hover:text-slate-700 hover:bg-slate-100 rounded-lg transition-colors"
              >
                <X className="w-3.5 h-3.5" />
                Clear
              </button>
            )}
          </div>
        </div>

        {/* Table Content */}
        <div className="min-h-[400px]">
          {showUnassignedOnly ? (
            // Unassigned Accounts View
            <UnassignedAccountsTable
              accounts={unassignedData?.accounts || []}
              isLoading={unassignedLoading}
            />
          ) : (
            // CSMs View
            csmsLoading ? (
              <div className="flex items-center justify-center py-12">
                <Loader2 className="w-6 h-6 text-primary-500 animate-spin" />
              </div>
            ) : filteredCSMs.length === 0 ? (
              <div className="text-center py-12 text-slate-500">
                <Users className="w-12 h-12 mx-auto mb-4 text-slate-300" />
                <p className="text-lg font-medium">No CSMs found</p>
                <p className="text-sm mt-1">
                  {csmStatusFilter 
                    ? `No ${csmStatusFilter} CSMs found. Try a different filter.`
                    : 'Try adjusting your search or filters'
                  }
                </p>
              </div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead className="bg-slate-50 sticky top-0">
                    <tr className="border-b border-slate-200">
                      <th className="w-12 py-3 px-4"></th>
                      <th className="text-left py-3 px-4 text-xs font-semibold text-slate-500 uppercase tracking-wider">CSM</th>
                      <th className="text-center py-3 px-4 text-xs font-semibold text-slate-500 uppercase tracking-wider">Type</th>
                      <th className="text-center py-3 px-4 text-xs font-semibold text-slate-500 uppercase tracking-wider">Accounts</th>
                      <th className="text-right py-3 px-4 text-xs font-semibold text-slate-500 uppercase tracking-wider">Total ARR</th>
                      <th className="text-right py-3 px-4 text-xs font-semibold text-slate-500 uppercase tracking-wider">Sub Tokens</th>
                      <th className="text-center py-3 px-4 text-xs font-semibold text-slate-500 uppercase tracking-wider">Needs Attention</th>
                      <th className="text-center py-3 px-4 text-xs font-semibold text-slate-500 uppercase tracking-wider relative group">
                        <span className="inline-flex items-center gap-1">
                          Workload
                          <Info className="w-3.5 h-3.5 text-slate-400 cursor-help" />
                        </span>
                        <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 w-64 p-3 bg-slate-800 text-white text-[11px] leading-relaxed rounded-lg shadow-lg opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none z-50 normal-case tracking-normal font-normal text-left">
                          <p className="font-semibold mb-1">FTE-Weighted Workload</p>
                          <p>Fair share is calculated by dividing total accounts by the sum of FTE weights across all active CSMs.</p>
                          <p className="mt-1"><span className="font-medium">Full-Time (FT):</span> 1.0× weight</p>
                          <p><span className="font-medium">Part-Time (PT):</span> 0.5× weight</p>
                          <p><span className="font-medium">Backfill (BF):</span> 0.5× weight</p>
                          <p className="mt-1 text-slate-300">100% = carrying their expected fair share based on type.</p>
                          <div className="absolute top-full left-1/2 -translate-x-1/2 -mt-px border-4 border-transparent border-t-slate-800" />
                        </div>
                      </th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-100">
                    {filteredCSMs.map((csm) => {
                      const type = csmTypes[csm.id] ?? 'full-time' as CSMType
                      const expected = fairSharePerFTE * FTE_WEIGHT[type]
                      return (
                        <CSMRow
                          key={csm.id}
                          csm={csm}
                          isExpanded={expandedCSMs.has(csm.id)}
                          onToggle={() => toggleCSM(csm.id)}
                          onViewProfile={() => setSelectedCSM(csm)}
                          expectedAccounts={expected}
                          csmType={type}
                          onTypeChange={handleTypeChange}
                          accountType={acctParam}
                        />
                      )
                    })}
                  </tbody>
                </table>
              </div>
            )
          )}
        </div>
      </div>

      {/* CSM Profile Panel */}
      {selectedCSM && (
        <CSMProfilePanel
          csm={selectedCSM}
          isOpen={!!selectedCSM}
          onClose={() => setSelectedCSM(null)}
        />
      )}
    </div>
  )
}

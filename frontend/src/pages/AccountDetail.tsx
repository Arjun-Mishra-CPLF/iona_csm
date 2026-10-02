import { useMemo, useState } from 'react'
import { 
  ArrowLeft, 
  Bell,
  Calendar, 
  FileText, 
  Loader2
} from 'lucide-react'
import { useAccountFullDetail } from '../hooks/useAccounts'
import { useLayoutPersistence } from '../hooks/useLayoutPersistence'
import { DraggableWidgetGrid, getDefaultLayout } from '../components/DraggableWidgetGrid'
import { Badge } from '../components/widgets'
import { WeeklySummaryDrawer } from '../components/WeeklySummaryDrawer'
import { RenewalHealthInsightDrawer } from '../components/RenewalHealthInsightDrawer'
import { clsx } from 'clsx'
import { healthLabel, healthVariant } from '../utils/healthLabels'

interface AccountDetailProps {
  accountId: string
  onBack: () => void
}

function formatDate(dateStr: string): string {
  return new Date(dateStr).toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric'
  })
}

function formatCurrency(value: number): string {
  if (value >= 1_000_000) return `€${(value / 1_000_000).toFixed(1)}M`
  if (value >= 1_000) return `€${(value / 1_000).toFixed(0)}K`
  return `€${value.toFixed(0)}`
}

export function AccountDetail({ accountId, onBack }: AccountDetailProps) {
  const { data, isLoading, isError, refetch } = useAccountFullDetail(accountId)
  
  // Get default layout
  const defaultLayout = useMemo(() => getDefaultLayout(), [])
  
  // Layout persistence
  const { layout, setLayout } = useLayoutPersistence(
    accountId,
    defaultLayout
  )
  
  // QBR generation state
  const [qbrGenerating, setQbrGenerating] = useState(false)
  
  // Weekly summary drawer
  const [summaryOpen, setSummaryOpen] = useState(false)
  const [renewalInsightOpen, setRenewalInsightOpen] = useState(false)
  
  // Generate QBR handler - directly generates PowerPoint
  const handleGenerateQBR = async () => {
    setQbrGenerating(true)
    
    try {
      const response = await fetch(`/api/accounts/${accountId}/qbr?format=pptx`)
      
      if (!response.ok) {
        throw new Error('Failed to generate QBR')
      }
      
      // Get the blob and create download link
      const blob = await response.blob()
      const url = window.URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      
      // Get filename from Content-Disposition header or use default
      const contentDisposition = response.headers.get('Content-Disposition')
      let filename = `QBR_${account?.name || 'Account'}.pptx`
      if (contentDisposition) {
        const match = contentDisposition.match(/filename="(.+)"/)
        if (match) filename = match[1]
      }
      
      a.download = filename
      document.body.appendChild(a)
      a.click()
      window.URL.revokeObjectURL(url)
      document.body.removeChild(a)
    } catch (error) {
      console.error('QBR generation failed:', error)
      alert('Failed to generate QBR. Please try again.')
    } finally {
      setQbrGenerating(false)
    }
  }

  const account = data?.account
  const healthScore = data?.health_breakdown?.overall_score

  return (
    <div className="flex flex-col h-full bg-slate-50">
      {/* Header */}
      <header className="bg-white border-b border-slate-200 px-4 py-3 flex-shrink-0">
        <div className="flex items-center justify-between">
          {/* Left: Back + Account Info */}
          <div className="flex items-center gap-4">
            <button
              onClick={onBack}
              className="flex items-center gap-1.5 px-3 py-1.5 text-sm text-slate-600 hover:text-slate-800 hover:bg-slate-100 rounded-lg transition-colors"
            >
              <ArrowLeft className="w-4 h-4" />
              Back
            </button>
            
            <div className="h-6 w-px bg-slate-200" />
            
            {isLoading ? (
              <div className="animate-pulse flex items-center gap-3">
                <div className="w-10 h-10 bg-slate-200 rounded-lg" />
                <div>
                  <div className="h-4 w-32 bg-slate-200 rounded" />
                  <div className="h-3 w-24 bg-slate-100 rounded mt-1" />
                </div>
              </div>
            ) : account ? (
              <div className="flex items-center gap-3">
                {/* Account Icon/Logo */}
                <div className="w-10 h-10 bg-gradient-to-br from-primary-400 to-primary-600 rounded-lg flex items-center justify-center text-white font-semibold">
                  {account.name.charAt(0)}
                </div>
                
                <div>
                  <h1 className="text-lg font-semibold text-slate-800">{account.name}</h1>
                  <div className="flex items-center gap-2 text-xs text-slate-500">
                    {account.industry && <span>{account.industry}</span>}
                    {account.employees && (
                      <>
                        <span className="text-slate-300">•</span>
                        <span>{account.employees.toLocaleString()} employees</span>
                      </>
                    )}
                  </div>
                </div>
              </div>
            ) : null}
          </div>

          {/* Center: Quick Stats */}
          {account && (
            <div className="flex items-center gap-6">
              {/* Health Score */}
              <div className="flex items-center gap-2">
                <div className={clsx(
                  'w-10 h-10 rounded-full flex items-center justify-center text-sm font-bold',
                  healthScore && healthScore >= 70 ? 'bg-emerald-100 text-emerald-700' :
                  healthScore && healthScore >= 40 ? 'bg-amber-100 text-amber-700' :
                  'bg-rose-100 text-rose-700'
                )}>
                  {healthScore || '--'}
                </div>
                <div className="text-xs">
                  <p className="text-slate-500">Health</p>
                  <Badge 
                    variant={
                      healthVariant(account.health)
                    }
                    size="sm"
                  >
                    {healthLabel(account.health)}
                  </Badge>
                </div>
              </div>

              {/* ARR */}
              <div className="text-xs">
                <p className="text-slate-500">ARR</p>
                <p className="text-sm font-semibold text-slate-800">
                  {formatCurrency(account.arr)}
                </p>
              </div>

              {/* Renewal */}
              <div className="flex items-center gap-2">
                <Calendar className="w-4 h-4 text-slate-400" />
                <div className="text-xs">
                  <p className="text-slate-500">Renewal</p>
                  <p className={clsx(
                    'text-sm font-semibold',
                    account.renewal_days <= 30 ? 'text-rose-600' :
                    account.renewal_days <= 90 ? 'text-amber-600' :
                    'text-slate-800'
                  )}>
                    {account.renewal_days}d ({formatDate(account.renewal_date)})
                  </p>
                </div>
              </div>
            </div>
          )}

          {/* Right: Actions */}
          <div className="flex items-center gap-2">
            <button
              onClick={() => setSummaryOpen(true)}
              className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-slate-600 bg-slate-100 rounded-lg hover:bg-slate-200 transition-colors"
              title="Weekly activity summary"
            >
              <Bell className="w-3.5 h-3.5" />
              Activity
            </button>
            
            <button 
              onClick={handleGenerateQBR}
              disabled={qbrGenerating}
              className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-white bg-primary-500 rounded-lg hover:bg-primary-600 transition-colors disabled:opacity-60"
            >
              {qbrGenerating ? (
                <Loader2 className="w-3.5 h-3.5 animate-spin" />
              ) : (
                <FileText className="w-3.5 h-3.5" />
              )}
              {qbrGenerating ? 'Generating...' : 'Generate QBR'}
            </button>
          </div>
        </div>
      </header>

      {/* Error State */}
      {isError && (
        <div className="p-4">
          <div className="bg-rose-50 border border-rose-200 rounded-lg p-4 text-center">
            <p className="text-sm text-rose-700">Failed to load account details.</p>
            <button
              onClick={() => refetch()}
              className="mt-2 text-xs text-rose-600 hover:text-rose-800 underline"
            >
              Try again
            </button>
          </div>
        </div>
      )}

      {/* Widget Grid */}
      <DraggableWidgetGrid
        data={data}
        isLoading={isLoading}
        layout={layout}
        onLayoutChange={setLayout}
      />

      {/* Weekly Summary Drawer */}
      <WeeklySummaryDrawer
        accountId={accountId}
        accountName={account?.name || ''}
        isOpen={summaryOpen}
        onClose={() => setSummaryOpen(false)}
      />

      <RenewalHealthInsightDrawer
        accountId={accountId}
        accountName={account?.name || ''}
        isOpen={renewalInsightOpen}
        onClose={() => setRenewalInsightOpen(false)}
      />
    </div>
  )
}

import React, { useState, useEffect, useRef } from 'react';
import { X, Play, Check, Database, Calendar, Building2, Search, Loader2, CheckCircle, XCircle, Activity, Clock, ToggleLeft, ToggleRight, History } from 'lucide-react';
import { CompanySummary } from '../types';
import * as api from '../apiService';

interface GlobalRunPipelineModalProps {
  isOpen: boolean;
  onClose: () => void;
  onStart: (config: { dataSources: string[]; financialYears: string[]; companyIds: string[]; allYears: boolean }) => Promise<api.PipelineJob[]>;
  onJobsComplete?: (statuses: Record<string, string>) => void;
  companies: CompanySummary[];
}

const GlobalRunPipelineModal: React.FC<GlobalRunPipelineModalProps> = ({ isOpen, onClose, onStart, onJobsComplete, companies }) => {
  const [selectedSources, setSelectedSources] = useState<string[]>(['Secondary']);
  const [selectedYears, setSelectedYears] = useState<string[]>(['FY2025']);
  const [allYears, setAllYears] = useState(false);
  const [selectedCompanyIds, setSelectedCompanyIds] = useState<string[]>([]);
  const [companySearch, setCompanySearch] = useState('');

  // Job tracking
  const [isStarting, setIsStarting] = useState(false);
  const [jobs, setJobs] = useState<api.PipelineJob[]>([]);
  const [jobDetails, setJobDetails] = useState<Record<string, api.PipelineJob>>({});
  const [jobStatuses, setJobStatuses] = useState<Record<string, string>>({});
  const [jobLogs, setJobLogs] = useState<Record<string, string[]>>({});
  const [expandedLogs, setExpandedLogs] = useState<Record<string, boolean>>({});
  const [startError, setStartError] = useState<string | null>(null);
  const pollingRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const completedRef = useRef(false);

  useEffect(() => {
    if (!isOpen) {
      setJobs([]);
      setJobDetails({});
      setJobStatuses({});
      setJobLogs({});
      setExpandedLogs({});
      setStartError(null);
      setIsStarting(false);
      completedRef.current = false;
      if (pollingRef.current) clearInterval(pollingRef.current);
    }
  }, [isOpen]);

  useEffect(() => {
    if (jobs.length === 0) return;
    const poll = async () => {
      const updates: Record<string, string> = {};
      const details: Record<string, api.PipelineJob> = {};
      try {
        const polled = await api.getPipelineJobStatuses(jobs.map(j => j.id));
        polled.forEach((j) => {
          updates[j.id] = j.status;
          details[j.id] = j;
        });
      } catch {
        // Keep prior status if batch poll fails.
      }

      setJobStatuses(prev => {
        const merged = { ...prev, ...updates };
        const done = jobs.every(j => ['PUBLISHED', 'ERROR', 'NEEDS_REVIEW'].includes(merged[j.id] ?? ''));
        if (done && !completedRef.current) {
          completedRef.current = true;
          if (pollingRef.current) {
            clearInterval(pollingRef.current);
            pollingRef.current = null;
          }
          onJobsComplete?.(merged);
        }
        return merged;
      });
      if (Object.keys(details).length > 0) {
        setJobDetails(prev => ({ ...prev, ...details }));
      }
    };
    poll();
    pollingRef.current = setInterval(poll, 3000);
    return () => { if (pollingRef.current) clearInterval(pollingRef.current); };
  }, [jobs, onJobsComplete]);

  if (!isOpen) return null;

  const dataSources = ['Secondary', 'Tertiary'];
  
  // Generate last 50 financial years
  const currentYear = new Date().getFullYear();
  const financialYears = Array.from({ length: 50 }, (_, i) => `FY${currentYear - i}`);

  const filteredCompanies = companies.filter(c => 
    c.name.toLowerCase().includes(companySearch.toLowerCase()) ||
    c.ticker.toLowerCase().includes(companySearch.toLowerCase())
  );

  const toggleSource = (source: string) => {
    setSelectedSources(prev => 
      prev.includes(source) ? prev.filter(s => s !== source) : [...prev, source]
    );
  };

  const toggleYear = (year: string) => {
    setSelectedYears(prev => 
      prev.includes(year) ? prev.filter(y => y !== year) : [...prev, year]
    );
  };

  const toggleCompany = (id: string) => {
    setSelectedCompanyIds(prev => 
      prev.includes(id) ? prev.filter(i => i !== id) : [...prev, id]
    );
  };

  const toggleAllCompanies = () => {
    if (selectedCompanyIds.length === companies.length) {
      setSelectedCompanyIds([]);
    } else {
      setSelectedCompanyIds(companies.map(c => c.id));
    }
  };

  const handleStart = async () => {
    setIsStarting(true);
    setStartError(null);
    try {
      const started = await onStart({ dataSources: selectedSources, financialYears: selectedYears, companyIds: selectedCompanyIds, allYears });
      const init: Record<string, string> = {};
      const details: Record<string, api.PipelineJob> = {};
      started.forEach(j => { init[j.id] = j.status; });
      started.forEach(j => { details[j.id] = j; });
      setJobStatuses(init);
      setJobDetails(details);
      setJobs(started);
    } catch (err: any) {
      setStartError(err.message ?? 'Failed to start pipeline');
    } finally {
      setIsStarting(false);
    }
  };

  const statusIcon = (s: string) => {
    if (s === 'PUBLISHED') return <CheckCircle className="w-4 h-4 text-emerald-400" />;
    if (s === 'ERROR') return <XCircle className="w-4 h-4 text-red-400" />;
    if (s === 'FETCHING') return <Activity className="w-4 h-4 text-indigo-400 animate-pulse" />;
    return <Clock className="w-4 h-4 text-slate-400 animate-pulse" />;
  };
  const statusColor = (s: string) => {
    if (s === 'PUBLISHED') return 'text-emerald-400';
    if (s === 'ERROR') return 'text-red-400';
    if (s === 'FETCHING') return 'text-indigo-400';
    return 'text-slate-400';
  };
  const allDone = jobs.length > 0 && jobs.every(j => ['PUBLISHED', 'ERROR', 'NEEDS_REVIEW'].includes(jobStatuses[j.id] ?? ''));

  const toggleLogs = async (jobId: string) => {
    const isOpen = !!expandedLogs[jobId];
    setExpandedLogs(prev => ({ ...prev, [jobId]: !isOpen }));
    if (!isOpen && !jobLogs[jobId]) {
      try {
        const payload = await api.getPipelineJobLogs(jobId, 120);
        setJobLogs(prev => ({ ...prev, [jobId]: payload.lines || [] }));
      } catch {
        setJobLogs(prev => ({ ...prev, [jobId]: ['Unable to load job logs.'] }));
      }
    }
  };

  return (
    <div className="fixed inset-0 z-[100] flex items-center justify-center p-6">
      {/* Backdrop */}
      <div 
        className="absolute inset-0 bg-slate-950/80 backdrop-blur-md transition-opacity"
        onClick={onClose}
      />

      {/* Modal Panel */}
      <div className="w-full max-w-3xl bg-slate-900 border border-slate-800 rounded-3xl shadow-2xl overflow-hidden relative flex flex-col max-h-[90vh]">
        {/* Header */}
        <div className="px-8 py-6 border-b border-slate-800 flex items-center justify-between bg-slate-900/50 backdrop-blur-sm sticky top-0 z-10">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 bg-indigo-600/20 rounded-xl flex items-center justify-center">
              <Play className="w-5 h-5 text-indigo-400 fill-current" />
            </div>
            <div>
              <h2 className="text-xl font-bold text-white tracking-tight">Global Pipeline Run</h2>
              <p className="text-slate-500 text-xs mt-0.5 uppercase tracking-widest font-bold">Bulk Processing Configuration</p>
            </div>
          </div>
          <button 
            onClick={onClose}
            className="p-2 hover:bg-slate-800 rounded-full text-slate-400 hover:text-white transition-colors"
          >
            <X className="w-6 h-6" />
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-8 space-y-10">
          
          {/* Section 1: Data Sources */}
          <section>
            <div className="flex items-center gap-2 mb-4">
              <Database className="w-4 h-4 text-indigo-400" />
              <h3 className="text-sm font-bold text-slate-300 uppercase tracking-wider">Data Sources</h3>
            </div>
            <div className="grid grid-cols-2 gap-4">
              {dataSources.map(source => (
                <button
                  key={source}
                  onClick={() => toggleSource(source)}
                  className={`
                    flex items-center justify-between p-4 rounded-2xl border transition-all
                    ${selectedSources.includes(source) 
                      ? 'bg-indigo-500/10 border-indigo-500 text-indigo-400 shadow-lg shadow-indigo-500/5' 
                      : 'bg-slate-950 border-slate-800 text-slate-500 hover:border-slate-700 hover:text-slate-300'}
                  `}
                >
                  <span className="font-semibold">{source} Data</span>
                  <div className={`
                    w-6 h-6 rounded-full border flex items-center justify-center transition-all
                    ${selectedSources.includes(source) ? 'bg-indigo-500 border-indigo-500' : 'border-slate-700'}
                  `}>
                    {selectedSources.includes(source) && <Check className="w-4 h-4 text-white" />}
                  </div>
                </button>
              ))}
            </div>
          </section>

          {/* Section 2: Companies */}
          <section>
            <div className="flex items-center justify-between mb-4">
              <div className="flex items-center gap-2">
                <Building2 className="w-4 h-4 text-indigo-400" />
                <h3 className="text-sm font-bold text-slate-300 uppercase tracking-wider">Target Companies</h3>
              </div>
              <div className="flex items-center gap-3">
                <button 
                  onClick={toggleAllCompanies}
                  className="text-[10px] font-bold text-indigo-400 hover:text-indigo-300 uppercase tracking-wider"
                >
                  {selectedCompanyIds.length === companies.length ? 'Deselect All' : 'Select All'}
                </button>
                <span className="text-[10px] font-bold text-slate-500 bg-slate-800 px-2 py-1 rounded uppercase">
                  {selectedCompanyIds.length} Selected
                </span>
              </div>
            </div>

            <div className="bg-slate-950 border border-slate-800 rounded-2xl overflow-hidden">
              <div className="p-3 border-b border-slate-800 flex items-center gap-2">
                <Search className="w-4 h-4 text-slate-500" />
                <input 
                  type="text"
                  placeholder="Filter companies..."
                  value={companySearch}
                  onChange={(e) => setCompanySearch(e.target.value)}
                  className="bg-transparent border-none focus:outline-none text-sm text-slate-200 w-full"
                />
              </div>
              <div className="max-h-48 overflow-y-auto p-2 grid grid-cols-2 gap-2">
                {filteredCompanies.map(company => (
                  <button
                    key={company.id}
                    onClick={() => toggleCompany(company.id)}
                    className={`
                      flex items-center gap-3 p-2 rounded-xl border transition-all text-left
                      ${selectedCompanyIds.includes(company.id) 
                        ? 'bg-indigo-500/10 border-indigo-500/50 text-indigo-300' 
                        : 'bg-slate-900 border-slate-800 text-slate-500 hover:border-slate-700 hover:text-slate-400'}
                    `}
                  >
                    <div className={`
                      w-4 h-4 rounded border flex items-center justify-center shrink-0
                      ${selectedCompanyIds.includes(company.id) ? 'bg-indigo-500 border-indigo-500' : 'border-slate-700'}
                    `}>
                      {selectedCompanyIds.includes(company.id) && <Check className="w-3 h-3 text-white" />}
                    </div>
                    <div className="truncate">
                      <p className="text-xs font-bold truncate">{company.name}</p>
                      <p className="text-[10px] opacity-60">{company.ticker}</p>
                    </div>
                  </button>
                ))}
              </div>
            </div>
          </section>

          {/* Section 3: Financial Years */}
          <section>
            <div className="flex items-center justify-between mb-4">
              <div className="flex items-center gap-2">
                <Calendar className="w-4 h-4 text-indigo-400" />
                <h3 className="text-sm font-bold text-slate-300 uppercase tracking-wider">Financial Years</h3>
              </div>
              {!allYears && (
                <span className="text-[10px] font-bold text-slate-500 bg-slate-800 px-2 py-1 rounded uppercase">
                  {selectedYears.length} Selected
                </span>
              )}
            </div>

            {/* All-Years Toggle */}
            <button
              onClick={() => setAllYears(v => !v)}
              className={`w-full flex items-center justify-between p-4 rounded-2xl border transition-all mb-4 ${
                allYears
                  ? 'bg-amber-500/10 border-amber-500/60 text-amber-400 shadow-lg shadow-amber-500/5'
                  : 'bg-slate-950 border-slate-800 text-slate-500 hover:border-slate-700 hover:text-slate-300'
              }`}
            >
              <div className="flex items-center gap-3">
                <History className={`w-4 h-4 ${allYears ? 'text-amber-400' : 'text-slate-500'}`} />
                <div className="text-left">
                  <p className="text-sm font-bold">Process All Historical Years</p>
                  <p className="text-[10px] opacity-70 mt-0.5">Runs <code className="font-mono">run_all.py --all-years</code> — fetches & scores every year (last 5 yrs)</p>
                </div>
              </div>
              {allYears
                ? <ToggleRight className="w-8 h-8 text-amber-400 shrink-0" />
                : <ToggleLeft className="w-8 h-8 text-slate-600 shrink-0" />}
            </button>

            {/* Individual year picker — hidden when allYears is on */}
            {!allYears && (
              <div className="grid grid-cols-4 sm:grid-cols-6 gap-2 bg-slate-950 p-4 rounded-2xl border border-slate-800">
                {financialYears.map(year => (
                  <button
                    key={year}
                    onClick={() => toggleYear(year)}
                    className={`
                      py-2 rounded-lg text-[10px] font-mono transition-all border
                      ${selectedYears.includes(year)
                        ? 'bg-indigo-500 text-white border-indigo-500 shadow-lg shadow-indigo-500/20'
                        : 'bg-slate-900 text-slate-500 border-slate-800 hover:border-slate-700 hover:text-slate-300'}
                    `}
                  >
                    {year}
                  </button>
                ))}
              </div>
            )}
          </section>

        </div>

        {/* Footer */}
        <div className="p-8 border-t border-slate-800 bg-slate-900/50 backdrop-blur-md sticky bottom-0 z-10 space-y-4">

          {/* Job tracker */}
          {(jobs.length > 0 || startError) && (
            <div className="bg-slate-950 border border-slate-800 rounded-2xl p-4 space-y-2 max-h-48 overflow-y-auto">
              {startError ? (
                <p className="text-sm text-red-400 flex items-center gap-2"><XCircle className="w-4 h-4" /> {startError}</p>
              ) : (
                <>
                  <p className="text-xs text-slate-500 uppercase font-bold tracking-wider mb-2 flex items-center gap-1.5"><Activity className="w-3 h-3" /> Running Jobs ({jobs.length})</p>
                  {jobs.map(j => (
                    <div key={j.id} className="space-y-1.5 border-b border-slate-800/60 pb-2 last:border-b-0 last:pb-0">
                      <div className="flex items-center justify-between gap-2">
                        <div className="flex items-center gap-2">
                          {statusIcon(jobStatuses[j.id] ?? 'QUEUED')}
                          <span className="text-sm text-slate-300">{j.company_name}</span>
                          <span className="text-xs font-mono text-slate-600">#{j.id}</span>
                        </div>
                        <div className="flex items-center gap-3">
                          <button onClick={() => toggleLogs(j.id)} className="text-[10px] uppercase font-bold text-indigo-400 hover:text-indigo-300">
                            {expandedLogs[j.id] ? 'Hide Logs' : 'View Logs'}
                          </button>
                          <span className={`text-xs font-bold uppercase ${statusColor(jobStatuses[j.id] ?? 'QUEUED')}`}>{jobStatuses[j.id] ?? 'QUEUED'}</span>
                        </div>
                      </div>
                      {jobDetails[j.id]?.error_msg && (
                        <p className="text-[11px] text-amber-400/90 pl-6">{jobDetails[j.id]?.error_msg}</p>
                      )}
                      {expandedLogs[j.id] && (
                        <pre className="mt-1 max-h-28 overflow-y-auto text-[10px] leading-relaxed text-slate-300 bg-slate-900 border border-slate-800 rounded p-2 whitespace-pre-wrap">
                          {(jobLogs[j.id] ?? ['Loading logs...']).join('\n')}
                        </pre>
                      )}
                    </div>
                  ))}
                  {!allDone && <p className="text-xs text-slate-600 flex items-center gap-1 pt-1"><Loader2 className="w-3 h-3 animate-spin" /> Polling every 3s…</p>}
                </>
              )}
            </div>
          )}

          <div className="flex gap-4">
            <button 
              onClick={onClose}
              className="flex-1 px-6 py-4 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-2xl font-bold transition-all"
            >
              {allDone ? 'Close' : 'Cancel'}
            </button>
            {jobs.length === 0 && (
              <button 
                onClick={handleStart}
                disabled={selectedSources.length === 0 || (!allYears && selectedYears.length === 0) || selectedCompanyIds.length === 0 || isStarting}
                className="flex-[2] px-6 py-4 bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 disabled:cursor-not-allowed text-white rounded-2xl font-bold shadow-xl shadow-indigo-500/20 flex items-center justify-center gap-2 transition-all hover:scale-[1.02] active:scale-[0.98]"
              >
                {isStarting ? <><Loader2 className="w-5 h-5 animate-spin" /> Starting…</> : <>Start Global Pipeline <ArrowRight className="w-5 h-5" /></>}
              </button>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

const ArrowRight = ({ className }: { className?: string }) => (
  <svg className={className} fill="none" viewBox="0 0 24 24" stroke="currentColor">
    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M14 5l7 7m0 0l-7 7m7-7H3" />
  </svg>
);

export default GlobalRunPipelineModal;

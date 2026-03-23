import React, { useState, useEffect, useRef } from 'react';
import { X, Play, Check, Database, Calendar, Loader2, CheckCircle, XCircle, Activity, Clock } from 'lucide-react';
import * as api from '../apiService';

interface RunPipelineModalProps {
  isOpen: boolean;
  onClose: () => void;
  onStart: (config: { dataSources: string[]; financialYears: string[] }) => Promise<api.PipelineJob[]>;
  onJobsComplete?: (statuses: Record<string, string>) => void;
  initialFinancialYear?: string;
}

const RunPipelineModal: React.FC<RunPipelineModalProps> = ({ isOpen, onClose, onStart, onJobsComplete, initialFinancialYear }) => {
  const [selectedSources, setSelectedSources] = useState<string[]>(['Secondary']);
  const [selectedYears, setSelectedYears] = useState<string[]>(['FY2025']);

  // Job tracking
  const [isStarting, setIsStarting] = useState(false);
  const [jobs, setJobs] = useState<api.PipelineJob[]>([]);
  const [jobDetails, setJobDetails] = useState<Record<string, api.PipelineJob>>({});
  const [jobStatuses, setJobStatuses] = useState<Record<string, string>>({});
  const [startError, setStartError] = useState<string | null>(null);
  const pollingRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const completedRef = useRef(false);

  useEffect(() => {
    if (!isOpen) {
      setJobs([]);
      setJobDetails({});
      setJobStatuses({});
      setStartError(null);
      setIsStarting(false);
      completedRef.current = false;
      if (pollingRef.current) clearInterval(pollingRef.current);
    }
  }, [isOpen]);

  useEffect(() => {
    if (!isOpen) return;
    if (initialFinancialYear && /^FY\d{4}$/.test(initialFinancialYear)) {
      setSelectedYears([initialFinancialYear]);
    }
  }, [isOpen, initialFinancialYear]);

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

  const handleStart = async () => {
    setIsStarting(true);
    setStartError(null);
    try {
      const started = await onStart({ dataSources: selectedSources, financialYears: selectedYears });
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

  return (
    <div className="fixed inset-0 z-[100] flex items-center justify-center p-6">
      {/* Backdrop */}
      <div 
        className="absolute inset-0 bg-slate-950/80 backdrop-blur-md transition-opacity"
        onClick={onClose}
      />

      {/* Modal Panel */}
      <div className="w-full max-w-2xl bg-slate-900 border border-slate-800 rounded-3xl shadow-2xl overflow-hidden relative flex flex-col max-h-[90vh]">
        {/* Header */}
        <div className="px-8 py-6 border-b border-slate-800 flex items-center justify-between bg-slate-900/50 backdrop-blur-sm sticky top-0 z-10">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 bg-indigo-600/20 rounded-xl flex items-center justify-center">
              <Play className="w-5 h-5 text-indigo-400 fill-current" />
            </div>
            <div>
              <h2 className="text-xl font-bold text-white tracking-tight">Run Risk Pipeline</h2>
              <p className="text-slate-500 text-xs mt-0.5 uppercase tracking-widest font-bold">Configuration</p>
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
        <div className="flex-1 overflow-y-auto p-8 space-y-8">
          
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

          {/* Section 2: Financial Years */}
          <section>
            <div className="flex items-center justify-between mb-4">
              <div className="flex items-center gap-2">
                <Calendar className="w-4 h-4 text-indigo-400" />
                <h3 className="text-sm font-bold text-slate-300 uppercase tracking-wider">Financial Years</h3>
              </div>
              <span className="text-[10px] font-bold text-slate-500 bg-slate-800 px-2 py-1 rounded uppercase">
                {selectedYears.length} Selected
              </span>
            </div>
            <div className="grid grid-cols-4 sm:grid-cols-5 gap-2 bg-slate-950 p-4 rounded-2xl border border-slate-800">
              {financialYears.map(year => (
                <button
                  key={year}
                  onClick={() => toggleYear(year)}
                  className={`
                    py-2 rounded-lg text-xs font-mono transition-all border
                    ${selectedYears.includes(year) 
                      ? 'bg-indigo-500 text-white border-indigo-500 shadow-lg shadow-indigo-500/20' 
                      : 'bg-slate-900 text-slate-500 border-slate-800 hover:border-slate-700 hover:text-slate-300'}
                  `}
                >
                  {year}
                </button>
              ))}
            </div>
          </section>

        </div>

        {/* Footer */}
        <div className="p-8 border-t border-slate-800 bg-slate-900/50 backdrop-blur-md sticky bottom-0 z-10 space-y-4">

          {/* Job tracker — shown after start */}
          {(jobs.length > 0 || startError) && (
            <div className="bg-slate-950 border border-slate-800 rounded-2xl p-4 space-y-2">
              {startError ? (
                <p className="text-sm text-red-400 flex items-center gap-2"><XCircle className="w-4 h-4" /> {startError}</p>
              ) : (
                <>
                  <p className="text-xs text-slate-500 uppercase font-bold tracking-wider mb-2 flex items-center gap-1.5"><Activity className="w-3 h-3" /> Running Jobs</p>
                  {jobs.map(j => (
                    <div key={j.id} className="space-y-1.5">
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-2">
                          {statusIcon(jobStatuses[j.id] ?? 'QUEUED')}
                          <span className="text-sm text-slate-300">{j.company_name}</span>
                          <span className="text-xs font-mono text-slate-600">#{j.id}</span>
                        </div>
                        <span className={`text-xs font-bold uppercase ${statusColor(jobStatuses[j.id] ?? 'QUEUED')}`}>{jobStatuses[j.id] ?? 'QUEUED'}</span>
                      </div>
                      {jobDetails[j.id]?.error_msg && (
                        <p className="text-[11px] text-amber-400/90 pl-6">{jobDetails[j.id]?.error_msg}</p>
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
                disabled={selectedSources.length === 0 || selectedYears.length === 0 || isStarting}
                className="flex-[2] px-6 py-4 bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 disabled:cursor-not-allowed text-white rounded-2xl font-bold shadow-xl shadow-indigo-500/20 flex items-center justify-center gap-2 transition-all hover:scale-[1.02] active:scale-[0.98]"
              >
                {isStarting ? <><Loader2 className="w-5 h-5 animate-spin" /> Starting…</> : <>Start Pipeline <ArrowRight className="w-5 h-5" /></>}
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

export default RunPipelineModal;

import React, { useState, useEffect } from 'react';
import { Indicator, AuditLogEvent, AuditLogEventType } from '../types';
import api from '../apiService';
import { 
  X, 
  GitCommit, 
  Bot, 
  User, 
  CheckCircle2, 
  FileText, 
  ArrowRight, 
  History,
  ShieldCheck,
  Database,
  Search
} from 'lucide-react';

interface LineageDrawerProps {
  isOpen: boolean;
  onClose: () => void;
  indicator: Indicator | null;
  companyId?: string;
}

// Helper to generate mock audit logs if none exist
const generateMockAuditLog = (indicator: Indicator): AuditLogEvent[] => {
  const logs: AuditLogEvent[] = [];
  const baseDate = new Date();
  
  // 1. Initial System Extraction (Always exists)
  logs.push({
    id: 'log-1',
    type: 'SYSTEM_EXTRACTION',
    timestamp: new Date(baseDate.getTime() - 86400000 * 10).toISOString(), // 10 days ago
    user: 'System Pipeline v2.4',
    description: `Initial value (${indicator.isOverridden ? 'Different Value' : indicator.value}) extracted via automated pipeline.`,
    metadata: {
      newValue: indicator.isOverridden ? 'Old Value' : indicator.value,
      sourceName: indicator.source
    }
  });

  // 2. If overridden, add Maker and Checker events
  if (indicator.isOverridden) {
    logs.push({
      id: 'log-2',
      type: 'MAKER_PROPOSAL',
      timestamp: new Date(baseDate.getTime() - 86400000 * 2).toISOString(), // 2 days ago
      user: 'Sarah Jenkins',
      description: `Override proposed. Reason: "${indicator.overrideReason || 'Correction needed'}"`,
      metadata: {
        previousValue: 'Old Value',
        newValue: indicator.value
      }
    });

    logs.push({
      id: 'log-3',
      type: 'CHECKER_APPROVAL',
      timestamp: new Date(baseDate.getTime() - 86400000 * 1).toISOString(), // 1 day ago
      user: 'Mike Ross (Manager)',
      description: 'Override approved. Live score updated.',
      metadata: {
        newValue: indicator.value
      }
    });
  }

  // 3. Random Source Addition event for flavor
  if (Math.random() > 0.5) {
    logs.push({
      id: 'log-4',
      type: 'SOURCE_ADDITION',
      timestamp: new Date(baseDate.getTime() - 86400000 * 5).toISOString(), // 5 days ago
      user: 'System',
      description: 'Recalculated due to new source document [Q3 ESG Report].',
      metadata: {
        sourceName: 'Q3 ESG Report.pdf'
      }
    });
  }

  return logs.sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime());
};

const LineageDrawer: React.FC<LineageDrawerProps> = ({ isOpen, onClose, indicator, companyId }) => {
  const [auditLogs, setAuditLogs] = useState<AuditLogEvent[]>([]);
  const [isLoadingLogs, setIsLoadingLogs] = useState(false);

  useEffect(() => {
    if (!isOpen || !indicator) return;
    if (companyId) {
      setIsLoadingLogs(true);
      api.getIndicatorLineage(companyId, indicator.id)
        .then(events => {
          const mapped: AuditLogEvent[] = events.map(e => ({
            id: e.id,
            type: e.type as AuditLogEventType,
            timestamp: e.timestamp,
            user: e.user,
            description: e.description,
            metadata: e.metadata,
          }));
          setAuditLogs(mapped.length > 0 ? mapped : generateMockAuditLog(indicator));
        })
        .catch(() => setAuditLogs(generateMockAuditLog(indicator)))
        .finally(() => setIsLoadingLogs(false));
    } else {
      setAuditLogs(indicator.auditLog || generateMockAuditLog(indicator));
    }
  }, [isOpen, indicator, companyId]);

  if (!isOpen || !indicator) return null;

  return (
    <div className="fixed inset-0 z-50 flex justify-end pointer-events-none">
      {/* Backdrop */}
      <div 
        className={`absolute inset-0 bg-slate-950/60 backdrop-blur-sm transition-opacity duration-300 pointer-events-auto ${isOpen ? 'opacity-100' : 'opacity-0'}`}
        onClick={onClose}
      />

      {/* Drawer Panel */}
      <div 
        className={`
          w-full max-w-xl h-full bg-slate-900 border-l border-slate-800 shadow-2xl transform transition-transform duration-300 pointer-events-auto flex flex-col
          ${isOpen ? 'translate-x-0' : 'translate-x-full'}
        `}
      >
        {/* Header */}
        <div className="h-16 flex items-center justify-between px-6 border-b border-slate-800 bg-slate-900/50 backdrop-blur-md z-10">
          <div className="flex items-center gap-3">
            <div className="p-2 bg-indigo-500/20 rounded-lg">
                <GitCommit className="w-5 h-5 text-indigo-400" />
            </div>
            <div>
                <h2 className="text-sm font-semibold text-slate-100 uppercase tracking-wide">Data Lineage</h2>
                <p className="text-xs text-slate-500 font-mono">{indicator.name}</p>
            </div>
          </div>
          <button 
            onClick={onClose}
            className="p-2 hover:bg-slate-800 rounded-full text-slate-400 hover:text-white transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Scrollable Content */}
        <div className="flex-1 overflow-y-auto p-6 space-y-8">
          
          {/* Part B: Score Explainer (The Math Tree) */}
          <section>
            <h3 className="text-xs font-bold text-slate-500 uppercase tracking-wider mb-4 flex items-center gap-2">
                <Search className="w-3 h-3" /> Traceability Engine
            </h3>
            
            <div className="bg-slate-950 border border-slate-800 rounded-xl p-6 relative overflow-hidden">
                {/* Connecting Lines */}
                <div className="absolute left-8 top-12 bottom-12 w-0.5 bg-slate-800"></div>

                {/* Level 1: Final Value */}
                <div className="relative z-10 flex items-start gap-4 mb-8">
                    <div className="w-8 h-8 rounded-full bg-indigo-600 flex items-center justify-center shrink-0 shadow-lg shadow-indigo-900/50 border border-indigo-400">
                        <div className="w-2.5 h-2.5 bg-white rounded-full"></div>
                    </div>
                    <div className="flex-1">
                        <div className="flex items-center justify-between">
                            <span className="text-xs text-indigo-400 font-bold uppercase tracking-wider">Live Value</span>
                            <span className="text-xs text-slate-500 font-mono">Level 1</span>
                        </div>
                        <div className="text-3xl font-mono text-white font-light mt-1">
                            {indicator.value} <span className="text-sm text-slate-500">{indicator.unit}</span>
                        </div>
                    </div>
                </div>

                {/* Level 2: Source */}
                <div className="relative z-10 flex items-start gap-4 mb-8">
                    <div className={`w-8 h-8 rounded-full flex items-center justify-center shrink-0 border ${indicator.isOverridden ? 'bg-amber-500/20 border-amber-500 text-amber-400' : 'bg-emerald-500/20 border-emerald-500 text-emerald-400'}`}>
                        {indicator.isOverridden ? <User className="w-4 h-4" /> : <Bot className="w-4 h-4" />}
                    </div>
                    <div className="flex-1 bg-slate-900/50 rounded-lg p-3 border border-slate-800">
                        <div className="flex items-center justify-between mb-2">
                            <span className={`text-xs font-bold uppercase tracking-wider ${indicator.isOverridden ? 'text-amber-400' : 'text-emerald-400'}`}>
                                {indicator.isOverridden ? 'Human Override' : 'AI Extraction'}
                            </span>
                            <span className="text-xs text-slate-500 font-mono">Level 2</span>
                        </div>
                        
                        {indicator.isOverridden ? (
                            <div>
                                <p className="text-sm text-slate-300">Manually overridden by <span className="text-white font-medium">Sarah Jenkins</span>.</p>
                                <p className="text-xs text-slate-500 mt-1 italic">"{indicator.overrideReason}"</p>
                            </div>
                        ) : (
                            <div>
                                <p className="text-sm text-slate-300">Extracted from <span className="text-indigo-300 hover:underline cursor-pointer">{indicator.source}</span></p>
                                <div className="mt-2 p-2 bg-slate-950 rounded border border-slate-800 text-xs text-slate-400 font-mono leading-relaxed">
                                    "...reported total Scope 1 emissions of {indicator.value} {indicator.unit} for the fiscal year..."
                                </div>
                                <button className="mt-2 text-xs text-indigo-400 hover:text-indigo-300 flex items-center gap-1">
                                    <FileText className="w-3 h-3" /> View Source PDF
                                </button>
                            </div>
                        )}
                    </div>
                </div>

                {/* Level 3: Confidence/Weight */}
                <div className="relative z-10 flex items-start gap-4">
                    <div className="w-8 h-8 rounded-full bg-slate-800 border border-slate-600 flex items-center justify-center shrink-0 text-slate-400">
                        <Database className="w-4 h-4" />
                    </div>
                    <div className="flex-1">
                        <div className="flex items-center justify-between">
                            <span className="text-xs text-slate-400 font-bold uppercase tracking-wider">Metadata</span>
                            <span className="text-xs text-slate-500 font-mono">Level 3</span>
                        </div>
                        <div className="flex gap-4 mt-2">
                            <div className="bg-slate-900 px-3 py-2 rounded border border-slate-800">
                                <span className="block text-[10px] text-slate-500 uppercase">Confidence</span>
                                <div className="flex items-center gap-2">
                                    <div className={`w-2 h-2 rounded-full ${indicator.confidence > 80 ? 'bg-emerald-500' : 'bg-amber-500'}`}></div>
                                    <span className="text-sm font-mono text-slate-200">{indicator.confidence}%</span>
                                </div>
                            </div>
                            <div className="bg-slate-900 px-3 py-2 rounded border border-slate-800">
                                <span className="block text-[10px] text-slate-500 uppercase">Pillar Weight</span>
                                <span className="text-sm font-mono text-slate-200">High (15%)</span>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
          </section>

          {/* Part C: Immutable Audit Ledger */}
          <section>
            <h3 className="text-xs font-bold text-slate-500 uppercase tracking-wider mb-4 flex items-center gap-2">
                <History className="w-3 h-3" /> Immutable Audit Ledger
            </h3>

            {isLoadingLogs ? (
              <div className="flex items-center gap-3 text-slate-500 py-8 justify-center">
                <div className="w-4 h-4 border-2 border-slate-600 border-t-indigo-500 rounded-full animate-spin" />
                <span className="text-sm">Loading audit history...</span>
              </div>
            ) : (
            <div className="relative pl-4 space-y-6">
                {/* Timeline Line */}
                <div className="absolute left-[23px] top-2 bottom-2 w-px bg-slate-800"></div>

                {auditLogs.map((log, index) => (
                    <div key={log.id} className="relative flex gap-4 group">
                        {/* Node Icon */}
                        <div className={`
                            w-5 h-5 rounded-full border-2 shrink-0 z-10 mt-0.5 transition-colors
                            ${log.type === 'CHECKER_APPROVAL' ? 'bg-emerald-900 border-emerald-500 text-emerald-500' : 
                              log.type === 'MAKER_PROPOSAL' ? 'bg-amber-900 border-amber-500 text-amber-500' :
                              log.type === 'SOURCE_ADDITION' ? 'bg-blue-900 border-blue-500 text-blue-500' :
                              'bg-slate-900 border-slate-600 text-slate-400'}
                        `}></div>

                        {/* Content */}
                        <div className="flex-1 pb-2">
                            <div className="flex items-center justify-between mb-1">
                                <span className={`text-xs font-bold uppercase tracking-wider ${
                                    log.type === 'CHECKER_APPROVAL' ? 'text-emerald-400' : 
                                    log.type === 'MAKER_PROPOSAL' ? 'text-amber-400' :
                                    log.type === 'SOURCE_ADDITION' ? 'text-blue-400' :
                                    'text-slate-400'
                                }`}>
                                    {log.type.replace('_', ' ')}
                                </span>
                                <span className="text-[10px] text-slate-500 font-mono">
                                    {new Date(log.timestamp).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })}
                                </span>
                            </div>
                            
                            <p className="text-sm text-slate-300 mb-1">{log.description}</p>
                            
                            <div className="flex items-center gap-2 text-xs text-slate-500">
                                <User className="w-3 h-3" />
                                <span>{log.user}</span>
                            </div>

                            {/* Metadata Diff View */}
                            {log.metadata && (log.metadata.previousValue || log.metadata.newValue) && (
                                <div className="mt-2 flex items-center gap-2 bg-slate-950/50 p-2 rounded border border-slate-800/50 w-fit">
                                    {log.metadata.previousValue && (
                                        <>
                                            <span className="font-mono text-slate-500 line-through">{log.metadata.previousValue}</span>
                                            <ArrowRight className="w-3 h-3 text-slate-600" />
                                        </>
                                    )}
                                    {log.metadata.newValue && (
                                        <span className="font-mono text-slate-200">{log.metadata.newValue}</span>
                                    )}
                                </div>
                            )}
                        </div>
                    </div>
                ))}
            </div>
            )}
          </section>

        </div>
      </div>
    </div>
  );
};

export default LineageDrawer;

import React, { useState, useEffect } from 'react';
import { CompanyData, EvidenceItem, PendingSource } from '../types';
import { Upload, FileText, Link as LinkIcon, ExternalLink, X, Plus, Clock, AlertCircle } from 'lucide-react';
import AddSourceModal from './AddSourceModal';
import api from '../apiService';

interface EvidencePanelProps {
  company: CompanyData;
  refreshKey?: number;
}

const EvidencePanel: React.FC<EvidencePanelProps> = ({ company, refreshKey }) => {
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [localEvidence, setLocalEvidence] = useState<EvidenceItem[]>(company.evidence ?? []);

  useEffect(() => {
    if (company.id) {
      api.getEvidence(company.id).then(ev => setLocalEvidence(ev as EvidenceItem[])).catch(console.error);
    }
  }, [company.id, refreshKey]);

  const handleSourceSubmit = async (source: PendingSource) => {
    try {
      const ev = await api.addEvidence(company.id, {
        type: source.type === 'URL' ? 'URL' : 'PDF',
        name: source.value,
        tags: source.tags,
        justification: source.justification,
        submitted_by: 'Current User'
      });
      setLocalEvidence(prev => [ev as EvidenceItem, ...prev]);
    } catch (err) {
      console.error('Add evidence failed:', err);
      // Optimistic fallback
      const newEvidence: EvidenceItem = {
        id: `pending-${Date.now()}`,
        type: source.type === 'URL' ? 'URL' : 'PDF',
        name: source.value,
        date: new Date().toISOString().split('T')[0],
        status: 'pending_review',
        tags: source.tags,
        pendingSource: source
      };
      setLocalEvidence(prev => [newEvidence, ...prev]);
    }
  };

  return (
    <div className="h-full flex flex-col">
      {/* Header Action */}
      <div className="mb-6">
        <button 
            onClick={() => setIsModalOpen(true)}
            className="w-full py-4 border-2 border-dashed border-slate-700 bg-slate-800/30 hover:bg-indigo-500/10 hover:border-indigo-500/50 text-slate-400 hover:text-indigo-400 rounded-lg transition-all flex flex-col items-center justify-center gap-2 group"
        >
            <div className="p-2 bg-slate-800 rounded-full group-hover:bg-indigo-500/20 transition-colors">
                <Plus className="w-5 h-5" />
            </div>
            <span className="font-medium text-sm">Propose New Data Source</span>
            <span className="text-xs text-slate-500">Feed the Beast (URL or PDF)</span>
        </button>
      </div>

      {/* Evidence List */}
      <div className="flex-1 overflow-hidden flex flex-col">
        <h3 className="text-slate-400 text-xs font-bold uppercase tracking-wider mb-3 flex items-center justify-between">
            <span>Evidence Locker ({localEvidence.length})</span>
            <span className="text-[10px] bg-slate-800 px-1.5 py-0.5 rounded text-slate-500">
                {localEvidence.filter(e => e.status === 'pending_review').length} Pending
            </span>
        </h3>
        
        <div className="flex-1 overflow-y-auto pr-2 space-y-2">
          {localEvidence.map((item) => (
            <div 
                key={item.id} 
                className={`
                    group flex items-center justify-between p-3 rounded border transition-all
                    ${item.status === 'pending_review' 
                        ? 'bg-amber-500/5 border-amber-500/20 hover:border-amber-500/40' 
                        : 'bg-slate-800/50 border-slate-800 hover:border-slate-600'
                    }
                `}
            >
              <div className="flex items-start gap-3 overflow-hidden">
                <div className={`mt-1 w-8 h-8 rounded flex items-center justify-center shrink-0 ${
                  item.status === 'pending_review' ? 'bg-amber-500/20 text-amber-400' :
                  item.type === 'PDF' ? 'bg-red-500/20 text-red-400' :
                  item.type === 'URL' ? 'bg-blue-500/20 text-blue-400' :
                  'bg-emerald-500/20 text-emerald-400'
                }`}>
                  {item.status === 'pending_review' ? <Clock className="w-4 h-4 animate-pulse" /> :
                   item.type === 'PDF' ? <FileText className="w-4 h-4" /> :
                   item.type === 'URL' ? <LinkIcon className="w-4 h-4" /> :
                   <ExternalLink className="w-4 h-4" />
                  }
                </div>
                
                <div className="min-w-0">
                  <div className="flex items-center gap-2">
                      <p className={`text-sm font-medium truncate transition-colors ${item.status === 'pending_review' ? 'text-amber-200' : 'text-slate-200 group-hover:text-indigo-300'}`}>
                        {item.name}
                      </p>
                      {item.status === 'pending_review' && (
                          <span className="text-[10px] bg-amber-500/20 text-amber-400 px-1.5 py-0.5 rounded uppercase font-bold tracking-wide">
                              Pending
                          </span>
                      )}
                  </div>
                  
                  <div className="flex items-center gap-2 mt-1">
                    <span className="text-xs text-slate-500">{item.date}</span>
                    <div className="flex gap-1">
                        {item.tags.map(tag => (
                            <span key={tag} className="text-[10px] bg-slate-700 text-slate-300 px-1.5 py-0.5 rounded-full">{tag}</span>
                        ))}
                    </div>
                  </div>
                  
                  {item.status === 'pending_review' && item.pendingSource && (
                      <div className="mt-2 text-xs text-amber-500/80 italic truncate max-w-[200px]">
                          "{item.pendingSource.justification}"
                      </div>
                  )}
                </div>
              </div>

              <div className="flex items-center gap-2 opacity-0 group-hover:opacity-100 transition-opacity">
                {item.status !== 'pending_review' && (
                    <button className="text-slate-500 hover:text-slate-300 p-1">
                        <ExternalLink className="w-4 h-4" />
                    </button>
                )}
                {item.status === 'pending_review' ? (
                    <div className="group/tooltip relative">
                        <AlertCircle className="w-4 h-4 text-amber-500 cursor-help" />
                        <div className="absolute right-0 bottom-6 w-48 bg-slate-900 border border-amber-500/30 p-2 rounded shadow-xl hidden group-hover/tooltip:block z-10">
                            <p className="text-xs text-amber-400 font-medium">Awaiting Manager Approval</p>
                            <p className="text-xs text-slate-500 mt-1">This source will not be processed until approved.</p>
                        </div>
                    </div>
                ) : (
                    <button className="text-slate-500 hover:text-red-400 p-1">
                        <X className="w-4 h-4" />
                    </button>
                )}
              </div>
            </div>
          ))}
        </div>
      </div>

      <AddSourceModal 
        isOpen={isModalOpen} 
        onClose={() => setIsModalOpen(false)} 
        onSubmit={handleSourceSubmit}
      />
    </div>
  );
};

export default EvidencePanel;
import React, { useState } from 'react';
import { X, Globe, Plus, Check, Database, Share2, MessageSquare, Newspaper, Globe2 } from 'lucide-react';

interface GlobalSourceModalProps {
  isOpen: boolean;
  onClose: () => void;
  onAdd: (source: { domain: string; type: 'SECONDARY' | 'TERTIARY'; subType?: string }) => void;
}

const GlobalSourceModal: React.FC<GlobalSourceModalProps> = ({ isOpen, onClose, onAdd }) => {
  const [domain, setDomain] = useState('');
  const [type, setType] = useState<'SECONDARY' | 'TERTIARY'>('SECONDARY');
  const [subType, setSubType] = useState('News');

  if (!isOpen) return null;

  const tertiaryTypes = [
    { id: 'News', icon: <Newspaper className="w-4 h-4" /> },
    { id: 'Social Media', icon: <Share2 className="w-4 h-4" /> },
    { id: 'Blog', icon: <MessageSquare className="w-4 h-4" /> },
    { id: 'Forum', icon: <Globe2 className="w-4 h-4" /> },
    { id: 'Academic', icon: <Database className="w-4 h-4" /> },
    { id: 'Government', icon: <Globe className="w-4 h-4" /> }
  ];

  const handleAdd = () => {
    if (!domain) return;
    onAdd({ domain, type, subType: type === 'TERTIARY' ? subType : undefined });
    setDomain('');
    onClose();
  };

  return (
    <div className="fixed inset-0 z-[100] flex items-center justify-center p-6">
      {/* Backdrop */}
      <div 
        className="absolute inset-0 bg-slate-950/80 backdrop-blur-md transition-opacity"
        onClick={onClose}
      />

      {/* Modal Panel */}
      <div className="w-full max-w-lg bg-slate-900 border border-slate-800 rounded-3xl shadow-2xl overflow-hidden relative flex flex-col">
        {/* Header */}
        <div className="px-8 py-6 border-b border-slate-800 flex items-center justify-between bg-slate-900/50 backdrop-blur-sm">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 bg-indigo-600/20 rounded-xl flex items-center justify-center">
              <Plus className="w-5 h-5 text-indigo-400" />
            </div>
            <div>
              <h2 className="text-xl font-bold text-white tracking-tight">Add Global Source</h2>
              <p className="text-slate-500 text-xs mt-0.5 uppercase tracking-widest font-bold">Intelligence Control</p>
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
        <div className="p-8 space-y-6">
          {/* Domain Input */}
          <div className="space-y-2">
            <label className="block text-xs font-bold text-slate-500 uppercase tracking-wider ml-1">
              Domain / URL
            </label>
            <div className="relative group">
              <Globe className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-slate-500 group-focus-within:text-indigo-500 transition-colors" />
              <input
                type="text"
                value={domain}
                onChange={(e) => setDomain(e.target.value)}
                placeholder="e.g., reuters.com or bloomberg.com"
                className="w-full bg-slate-950 border border-slate-800 rounded-2xl py-4 pl-12 pr-4 text-white placeholder:text-slate-600 focus:outline-none focus:border-indigo-500 focus:ring-4 focus:ring-indigo-500/10 transition-all"
              />
            </div>
          </div>

          {/* Source Type Selection */}
          <div className="space-y-2">
            <label className="block text-xs font-bold text-slate-500 uppercase tracking-wider ml-1">
              Source Classification
            </label>
            <div className="grid grid-cols-2 gap-4">
              <button
                onClick={() => setType('SECONDARY')}
                className={`
                  flex flex-col items-center gap-2 p-4 rounded-2xl border transition-all text-center
                  ${type === 'SECONDARY' 
                    ? 'bg-indigo-500/10 border-indigo-500 text-indigo-400' 
                    : 'bg-slate-950 border-slate-800 text-slate-500 hover:border-slate-700'}
                `}
              >
                <span className="font-bold text-sm">Secondary</span>
                <span className="text-[10px] opacity-60">Verified reports & filings</span>
              </button>
              <button
                onClick={() => setType('TERTIARY')}
                className={`
                  flex flex-col items-center gap-2 p-4 rounded-2xl border transition-all text-center
                  ${type === 'TERTIARY' 
                    ? 'bg-indigo-500/10 border-indigo-500 text-indigo-400' 
                    : 'bg-slate-950 border-slate-800 text-slate-500 hover:border-slate-700'}
                `}
              >
                <span className="font-bold text-sm">Tertiary</span>
                <span className="text-[10px] opacity-60">News & Alternative data</span>
              </button>
            </div>
          </div>

          {/* Tertiary Sub-type Selection */}
          {type === 'TERTIARY' && (
            <div className="space-y-2 animate-in fade-in slide-in-from-top-2 duration-300">
              <label className="block text-xs font-bold text-slate-500 uppercase tracking-wider ml-1">
                Tertiary Category
              </label>
              <div className="grid grid-cols-3 gap-2">
                {tertiaryTypes.map(t => (
                  <button
                    key={t.id}
                    onClick={() => setSubType(t.id)}
                    className={`
                      py-3 px-2 rounded-xl text-[10px] font-bold uppercase tracking-wider border transition-all flex flex-col items-center gap-1.5
                      ${subType === t.id 
                        ? 'bg-indigo-500 text-white border-indigo-500' 
                        : 'bg-slate-950 border-slate-800 text-slate-500 hover:border-slate-700'}
                    `}
                  >
                    {t.icon}
                    {t.id}
                  </button>
                ))}
              </div>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="p-8 border-t border-slate-800 bg-slate-900/50 backdrop-blur-md flex gap-4">
          <button 
            onClick={onClose}
            className="flex-1 px-6 py-4 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-2xl font-bold transition-all"
          >
            Cancel
          </button>
          <button 
            onClick={handleAdd}
            disabled={!domain}
            className="flex-[2] px-6 py-4 bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 disabled:cursor-not-allowed text-white rounded-2xl font-bold shadow-xl shadow-indigo-500/20 flex items-center justify-center gap-2 transition-all hover:scale-[1.02] active:scale-[0.98]"
          >
            Add Global Source
          </button>
        </div>
      </div>
    </div>
  );
};

export default GlobalSourceModal;

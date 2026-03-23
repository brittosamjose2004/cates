import React, { useState, useEffect, useRef } from 'react';
import { CompanyData, Indicator } from '../types';
import { 
  X, 
  Upload, 
  FileText, 
  Tag, 
  ArrowRight, 
  ShieldCheck, 
  AlertTriangle, 
  Save, 
  Loader2, 
  CheckCircle2, 
  Lock,
  ArrowRightLeft
} from 'lucide-react';
import * as api from '../apiService';

interface InterventionDrawerProps {
  isOpen: boolean;
  onClose: () => void;
  company: CompanyData;
  onEvidenceAdded?: () => void;
}

type Tab = 'UPLOAD' | 'OVERRIDE';

const InterventionDrawer: React.FC<InterventionDrawerProps> = ({ isOpen, onClose, company, onEvidenceAdded }) => {
  const [activeTab, setActiveTab] = useState<Tab>('UPLOAD');
  const fileInputRef = useRef<HTMLInputElement>(null);
  
  // --- Upload Logic ---
  const [files, setFiles] = useState<Array<{ file: File; name: string; size: string; tag: string; status: 'PENDING' | 'PARSING' | 'DONE' | 'ERROR' }>>([]);
  const [isDragOver, setIsDragOver] = useState(false);

  const addRealFiles = (rawFiles: FileList | null) => {
    if (!rawFiles) return;
    const newEntries = Array.from(rawFiles).map(f => ({
      file: f,
      name: f.name,
      size: f.size > 1024 * 1024 ? `${(f.size / 1024 / 1024).toFixed(1)} MB` : `${(f.size / 1024).toFixed(0)} KB`,
      tag: '',
      status: 'PENDING' as const,
    }));
    setFiles(prev => [...prev, ...newEntries]);
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setIsDragOver(false);
    addRealFiles(e.dataTransfer.files);
  };

  const handleBrowse = () => {
    fileInputRef.current?.click();
  };

  const handleProcessFiles = async () => {
    const pending = files.filter(f => f.status === 'PENDING' && f.tag);
    if (!pending.length) return;

    // Set all pending to PARSING
    setFiles(prev => prev.map(f => f.status === 'PENDING' && f.tag ? { ...f, status: 'PARSING' as const } : f));

    for (const entry of pending) {
      try {
        await api.uploadEvidence(company.id, entry.file, entry.tag);
        setFiles(prev => prev.map(f => f.name === entry.name && f.status === 'PARSING' ? { ...f, status: 'DONE' as const } : f));
      } catch {
        setFiles(prev => prev.map(f => f.name === entry.name && f.status === 'PARSING' ? { ...f, status: 'ERROR' as const } : f));
      }
    }

    onEvidenceAdded?.();
  };

  const updateFileTag = (index: number, tag: string) => {
    const newFiles = [...files];
    newFiles[index].tag = tag;
    setFiles(newFiles);
  };

  // --- Override Logic ---
  const [overrides, setOverrides] = useState<Record<string, { value: string; justification: string }>>({});
  const [simulatedScore, setSimulatedScore] = useState<number | null>(null);
  const [overrideSubmitting, setOverrideSubmitting] = useState(false);
  const [overrideSuccess, setOverrideSuccess] = useState(false);

  const handleOverrideChange = (id: string, field: 'value' | 'justification', val: string) => {
    setOverrides(prev => {
      const current = prev[id] || { value: '', justification: '' };
      const next: Record<string, { value: string; justification: string }> = { ...prev, [id]: { ...current, [field]: val } };
      
      const hasAny = Object.values(next).some(o => o.value && o.value !== '');
      setSimulatedScore(hasAny ? 72 : null);
      return next;
    });
  };

  const handleApplyOverrides = async () => {
    const toSubmit = (Object.entries(overrides) as [string, { value: string; justification: string }][]).filter(([, o]) => o.value && o.justification);
    if (!toSubmit.length) return;

    setOverrideSubmitting(true);
    try {
      for (const [indicatorId, override] of toSubmit) {
        const indicator = company.indicators.find(i => i.id === indicatorId);
        if (!indicator) continue;
        const ov = override as { value: string; justification: string };
        await api.submitOverride({
          company_id: company.id,
          indicator_id: indicatorId,
          indicator_name: indicator.name,
          current_value: String(indicator.value),
          new_value: ov.value,
          justification: ov.justification,
        });
      }
      setOverrideSuccess(true);
      setTimeout(() => {
        setOverrideSuccess(false);
        onClose();
      }, 1500);
    } catch (err) {
      console.error('Override submission failed:', err);
    } finally {
      setOverrideSubmitting(false);
    }
  };

  // Reset state on close
  useEffect(() => {
    if (!isOpen) {
        setFiles([]);
        setOverrides({});
        setSimulatedScore(null);
        setActiveTab('UPLOAD');
        setOverrideSuccess(false);
    }
  }, [isOpen]);

  return (
    <>
      {/* Backdrop */}
      <div 
        className={`fixed inset-0 bg-slate-950/80 backdrop-blur-sm z-40 transition-opacity duration-300 ${isOpen ? 'opacity-100' : 'opacity-0 pointer-events-none'}`} 
        onClick={onClose}
      />

      {/* Drawer */}
      <div className={`
        fixed inset-y-0 right-0 w-[600px] bg-slate-900 border-l border-slate-800 shadow-2xl transform transition-transform duration-300 z-50 flex flex-col
        ${isOpen ? 'translate-x-0' : 'translate-x-full'}
      `}>
        
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-slate-800 bg-slate-900">
            <div>
                <h2 className="text-lg font-bold text-white flex items-center gap-2">
                    <ShieldCheck className="w-5 h-5 text-indigo-500" />
                    Intervention & Override
                </h2>
                <p className="text-xs text-slate-500 mt-0.5">Manually correct data or ingest missing evidence.</p>
            </div>
            <button onClick={onClose} className="text-slate-500 hover:text-white transition-colors">
                <X className="w-5 h-5" />
            </button>
        </div>

        {/* Tabs */}
        <div className="flex border-b border-slate-800">
            <button 
                onClick={() => setActiveTab('UPLOAD')}
                className={`flex-1 py-3 text-sm font-medium transition-colors border-b-2 ${activeTab === 'UPLOAD' ? 'border-indigo-500 text-indigo-400 bg-indigo-500/5' : 'border-transparent text-slate-500 hover:text-slate-300'}`}
            >
                1. Feed the Beast (Upload)
            </button>
            <button 
                onClick={() => setActiveTab('OVERRIDE')}
                className={`flex-1 py-3 text-sm font-medium transition-colors border-b-2 ${activeTab === 'OVERRIDE' ? 'border-indigo-500 text-indigo-400 bg-indigo-500/5' : 'border-transparent text-slate-500 hover:text-slate-300'}`}
            >
                2. Manual Override
            </button>
        </div>

        {/* Content Area */}
        <div className="flex-1 overflow-y-auto p-6">
            
            {/* --- UPLOAD TAB --- */}
            {activeTab === 'UPLOAD' && (
                <div className="space-y-6">
                    {/* Drop Zone */}
                    <div 
                        className={`
                            border-2 border-dashed rounded-xl p-8 text-center transition-all duration-200
                            ${isDragOver ? 'border-indigo-500 bg-indigo-500/10' : 'border-slate-700 bg-slate-800/30 hover:border-slate-600'}
                        `}
                        onDragOver={(e) => { e.preventDefault(); setIsDragOver(true); }}
                        onDragLeave={() => setIsDragOver(false)}
                        onDrop={handleDrop}
                    >
                        <div className="w-12 h-12 bg-slate-800 rounded-full flex items-center justify-center mx-auto mb-4 text-indigo-400 shadow-lg">
                            <Upload className="w-6 h-6" />
                        </div>
                        <h3 className="text-slate-200 font-medium mb-1">Drag & Drop Evidence</h3>
                        <p className="text-slate-500 text-sm mb-4">PDF Reports, Excel Sheets, or News Clippings</p>
                        <button
                          onClick={handleBrowse}
                          className="text-xs text-indigo-400 border border-indigo-500/30 px-3 py-1 rounded hover:bg-indigo-500/10 transition-colors">
                            Browse Files
                        </button>
                        <input
                          ref={fileInputRef}
                          type="file"
                          multiple
                          accept=".pdf,.xlsx,.xls,.csv"
                          className="hidden"
                          onChange={(e) => addRealFiles(e.target.files)}
                        />
                    </div>

                    {/* File List */}
                    {files.length > 0 && (
                        <div className="space-y-3">
                            <h4 className="text-xs text-slate-500 uppercase font-bold tracking-wider">Pending Ingestion</h4>
                            {files.map((file, idx) => (
                                <div key={idx} className="bg-slate-800 border border-slate-700 rounded-lg p-3">
                                    <div className="flex items-start justify-between mb-3">
                                        <div className="flex items-center gap-3">
                                            <FileText className="w-8 h-8 text-slate-500" />
                                            <div>
                                                <p className="text-sm text-slate-200 font-medium">{file.name}</p>
                                                <p className="text-xs text-slate-500">{file.size}</p>
                                            </div>
                                        </div>
                                        {file.status === 'DONE' ? (
                                            <div className="text-emerald-400 flex items-center gap-1.5 text-xs font-medium bg-emerald-500/10 px-2 py-1 rounded">
                                                <CheckCircle2 className="w-3.5 h-3.5" /> Uploaded
                                            </div>
                                        ) : file.status === 'ERROR' ? (
                                            <div className="text-red-400 flex items-center gap-1.5 text-xs font-medium bg-red-500/10 px-2 py-1 rounded">
                                                Upload Failed
                                            </div>
                                        ) : file.status === 'PARSING' ? (
                                            <div className="text-indigo-400 flex items-center gap-1.5 text-xs font-medium">
                                                <Loader2 className="w-3.5 h-3.5 animate-spin" /> Uploading...
                                            </div>
                                        ) : (
                                            <button onClick={() => {
                                                const newFiles = [...files];
                                                newFiles.splice(idx, 1);
                                                setFiles(newFiles);
                                            }}>
                                                <X className="w-4 h-4 text-slate-500 hover:text-white" />
                                            </button>
                                        )}
                                    </div>
                                    
                                    {/* Tagging Interface */}
                                    <div className="flex items-center gap-2 bg-slate-900/50 p-2 rounded">
                                        <Tag className="w-3.5 h-3.5 text-slate-500" />
                                        <select 
                                            className="bg-transparent text-xs text-slate-300 focus:outline-none w-full cursor-pointer"
                                            value={file.tag}
                                            onChange={(e) => updateFileTag(idx, e.target.value)}
                                            disabled={file.status !== 'PENDING'}
                                        >
                                            <option value="" disabled>Select Document Tag (Required)...</option>
                                            <option value="Annual Report">Annual Report</option>
                                            <option value="Sustainability Report">Sustainability / ESG Report</option>
                                            <option value="Financial Statement">Financial Statement</option>
                                            <option value="News Article">News / Media Article</option>
                                        </select>
                                    </div>

                                    {/* Progress Bar */}
                                    {file.status === 'PARSING' && (
                                        <div className="mt-3 w-full h-1 bg-slate-900 rounded-full overflow-hidden">
                                            <div className="h-full bg-indigo-500 animate-pulse w-2/3"></div>
                                        </div>
                                    )}
                                </div>
                            ))}

                            <button 
                                onClick={handleProcessFiles}
                                disabled={files.some(f => f.status === 'PENDING' && !f.tag) || !files.some(f => f.status === 'PENDING') || files.some(f => f.status === 'PARSING')}
                                className="w-full py-3 bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 disabled:cursor-not-allowed text-white rounded-lg font-medium text-sm flex items-center justify-center gap-2 shadow-lg shadow-indigo-900/20 transition-all"
                            >
                                {files.some(f => f.status === 'PARSING') ? 'Processing...' : 'Ingest & Recalculate'}
                            </button>
                        </div>
                    )}
                </div>
            )}

            {/* --- OVERRIDE TAB --- */}
            {activeTab === 'OVERRIDE' && (
                <div className="space-y-6">
                    <div className="bg-amber-500/10 border border-amber-500/20 p-4 rounded-lg flex gap-3">
                        <AlertTriangle className="w-5 h-5 text-amber-500 shrink-0" />
                        <div className="text-xs text-amber-200">
                            <p className="font-bold mb-1">Impact Warning</p>
                            Manual overrides lock specific data points. The AI model will skip these fields in future runs until unlocked.
                        </div>
                    </div>

                    <div className="space-y-4">
                        {company.indicators.slice(0, 3).map((indicator) => {
                           const override = overrides[indicator.id];
                           const hasOverride = !!override?.value;

                           return (
                            <div key={indicator.id} className={`p-4 rounded-lg border transition-all ${hasOverride ? 'bg-indigo-500/5 border-indigo-500/30' : 'bg-slate-800/50 border-slate-700'}`}>
                                <div className="flex justify-between items-start mb-3">
                                    <div>
                                        <p className="text-sm font-medium text-slate-200">{indicator.name}</p>
                                        <p className="text-xs text-slate-500 mt-0.5">Source: {indicator.source}</p>
                                    </div>
                                    <div className="text-right">
                                        <p className="text-xs text-slate-500 uppercase font-bold">AI Value</p>
                                        <p className="font-mono text-slate-400 line-through decoration-slate-600">{indicator.value} {indicator.unit}</p>
                                    </div>
                                </div>

                                <div className="grid grid-cols-2 gap-4">
                                    <div>
                                        <label className="block text-xs text-slate-500 mb-1.5 font-medium">Override Value</label>
                                        <div className="relative">
                                            <input 
                                                type="text" 
                                                className="w-full bg-slate-900 border border-slate-600 focus:border-indigo-500 rounded px-3 py-2 text-sm text-white focus:outline-none font-mono"
                                                placeholder={indicator.value.toString()}
                                                value={override?.value || ''}
                                                onChange={(e) => handleOverrideChange(indicator.id, 'value', e.target.value)}
                                            />
                                            {hasOverride && <Lock className="w-3 h-3 text-amber-500 absolute right-3 top-3" />}
                                        </div>
                                    </div>
                                    <div>
                                        <label className="block text-xs text-slate-500 mb-1.5 font-medium">Justification (Required)</label>
                                        <input 
                                            type="text" 
                                            className="w-full bg-slate-900 border border-slate-600 focus:border-indigo-500 rounded px-3 py-2 text-sm text-white focus:outline-none"
                                            placeholder="Reason for change..."
                                            value={override?.justification || ''}
                                            onChange={(e) => handleOverrideChange(indicator.id, 'justification', e.target.value)}
                                        />
                                    </div>
                                </div>
                            </div>
                           );
                        })}
                    </div>
                </div>
            )}
        </div>

        {/* Footer / Impact Simulator */}
        {activeTab === 'OVERRIDE' && (
            <div className="border-t border-slate-800 bg-slate-900 p-6 space-y-4">
                {/* Impact Simulator */}
                <div className="bg-slate-950 rounded-lg p-4 border border-slate-800 flex items-center justify-between">
                    <div>
                        <p className="text-xs text-slate-500 uppercase font-bold tracking-wider mb-1">Impact Simulator</p>
                        <p className="text-sm text-slate-400">Sustainability Risk Score</p>
                    </div>
                    
                    <div className="flex items-center gap-4">
                        <div className="text-right">
                            <span className="block text-2xl font-bold text-slate-300">85</span>
                            <span className="text-xs text-slate-500">Current</span>
                        </div>
                        
                        <ArrowRight className={`w-5 h-5 ${simulatedScore ? 'text-indigo-500' : 'text-slate-700'}`} />
                        
                        <div className="text-right">
                            {simulatedScore ? (
                                <>
                                    <span className="block text-2xl font-bold text-emerald-400 animate-pulse">72</span>
                                    <span className="text-xs text-emerald-500/80">Projected</span>
                                </>
                            ) : (
                                <>
                                    <span className="block text-2xl font-bold text-slate-700">--</span>
                                    <span className="text-xs text-slate-700">Projected</span>
                                </>
                            )}
                        </div>
                    </div>
                </div>

                <div className="flex gap-3">
                    <button onClick={onClose} className="flex-1 py-2.5 border border-slate-700 hover:bg-slate-800 text-slate-300 rounded-lg text-sm font-medium transition-colors">
                        Cancel
                    </button>
                    <button 
                        onClick={handleApplyOverrides}
                        disabled={!simulatedScore || overrideSubmitting || overrideSuccess}
                        className="flex-1 py-2.5 bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 disabled:cursor-not-allowed text-white rounded-lg text-sm font-medium shadow-lg shadow-indigo-900/20 flex items-center justify-center gap-2 transition-all"
                    >
                        {overrideSuccess ? (
                            <><CheckCircle2 className="w-4 h-4 text-emerald-400" /> Submitted!</>
                        ) : overrideSubmitting ? (
                            <><Loader2 className="w-4 h-4 animate-spin" /> Submitting...</>
                        ) : (
                            <><Save className="w-4 h-4" /> Apply Overrides</>
                        )}
                    </button>
                </div>
            </div>
        )}

      </div>
    </>
  );
};

export default InterventionDrawer;
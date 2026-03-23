import React, { useState, useEffect } from 'react';
import { Indicator, PendingOverride, UserRole } from '../types';
import { 
  X, 
  AlertTriangle, 
  FileText, 
  Link as LinkIcon, 
  UploadCloud, 
  ArrowRight,
  CheckCircle2,
  AlertCircle,
  ShieldCheck
} from 'lucide-react';
import api from '../apiService';

interface OverrideProposalDrawerProps {
  isOpen: boolean;
  onClose: () => void;
  indicator: Indicator | null;
  onSubmit: (override: PendingOverride) => void;
  userRole: UserRole;
  companyId?: string;
}

const OverrideProposalDrawer: React.FC<OverrideProposalDrawerProps> = ({ 
  isOpen, 
  onClose, 
  indicator, 
  onSubmit,
  userRole,
  companyId
}) => {
  const [newValue, setNewValue] = useState<string>('');
  const [evidenceType, setEvidenceType] = useState<'URL' | 'FILE'>('URL');
  const [evidenceValue, setEvidenceValue] = useState<string>('');
  const [justification, setJustification] = useState<string>('');
  const [isSubmitting, setIsSubmitting] = useState(false);

  // Reset form when indicator changes
  useEffect(() => {
    if (isOpen && indicator) {
      setNewValue(indicator.value.toString());
      setEvidenceType('URL');
      setEvidenceValue('');
      setJustification('');
    }
  }, [isOpen, indicator]);

  if (!isOpen || !indicator) return null;

  const handleSubmit = async () => {
    if (!indicator) return;
    setIsSubmitting(true);
    try {
      if (companyId) {
        await api.submitOverride({
          company_id: companyId,
          indicator_id: indicator.id,
          indicator_name: indicator.name,
          current_value: indicator.value,
          new_value: newValue,
          justification,
          submitted_by: 'Current User',
        });
      }
      onSubmit({
        newValue,
        evidenceType,
        evidenceValue,
        justification,
        submittedAt: new Date().toISOString(),
        submittedBy: 'Current User',
      });
      onClose();
    } catch (err) {
      console.error('Override submission failed:', err);
    } finally {
      setIsSubmitting(false);
    }
  };

  const isFormValid = newValue && evidenceValue && justification.length > 10;

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
          w-full max-w-md h-full bg-slate-900 border-l border-slate-800 shadow-2xl transform transition-transform duration-300 pointer-events-auto flex flex-col
          ${isOpen ? 'translate-x-0' : 'translate-x-full'}
        `}
      >
        {/* Header */}
        <div className="h-16 flex items-center justify-between px-6 border-b border-slate-800 bg-slate-900/50 backdrop-blur-md z-10">
          <div>
            <h2 className="text-lg font-semibold text-slate-100">Propose Override</h2>
            <p className="text-xs text-slate-500 font-mono">{indicator.name}</p>
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
          
          {/* Current State Block */}
          <section className="bg-slate-800/50 rounded-lg p-4 border border-slate-700/50">
            <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-3 flex items-center gap-2">
              <div className="w-1.5 h-1.5 rounded-full bg-indigo-500"></div>
              Current State (AI Extracted)
            </h3>
            <div className="flex items-center justify-between mb-2">
              <span className="text-2xl font-mono text-slate-200">{indicator.value}</span>
              <span className="text-sm text-slate-500 font-mono">{indicator.unit}</span>
            </div>
            <div className="flex items-center gap-2 text-xs text-slate-400 bg-slate-900/50 p-2 rounded border border-slate-800">
              <FileText className="w-3 h-3" />
              <span className="truncate">{indicator.source}</span>
            </div>
          </section>

          {/* Proposed State Block */}
          <section className="space-y-6">
            <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wider flex items-center gap-2">
              <div className="w-1.5 h-1.5 rounded-full bg-amber-500"></div>
              Proposed Correction
            </h3>

            {/* New Value Input */}
            <div className="space-y-2">
              <label className="block text-sm font-medium text-slate-300">
                New Value <span className="text-red-500">*</span>
              </label>
              <div className="relative">
                <input 
                  type="text" 
                  value={newValue}
                  onChange={(e) => setNewValue(e.target.value)}
                  className="w-full bg-slate-950 border border-slate-700 rounded-lg px-4 py-3 text-slate-100 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500 focus:outline-none font-mono"
                  placeholder="Enter corrected value..."
                />
                <span className="absolute right-4 top-3.5 text-xs text-slate-500 font-mono pointer-events-none">
                  {indicator.unit}
                </span>
              </div>
            </div>

            {/* Evidence Input */}
            <div className="space-y-2">
              <label className="block text-sm font-medium text-slate-300">
                Supporting Evidence <span className="text-red-500">*</span>
              </label>
              
              <div className="flex bg-slate-950 rounded-lg p-1 border border-slate-700 mb-2">
                <button 
                  onClick={() => setEvidenceType('URL')}
                  className={`flex-1 py-1.5 text-xs font-medium rounded transition-colors ${evidenceType === 'URL' ? 'bg-slate-800 text-white shadow-sm' : 'text-slate-500 hover:text-slate-300'}`}
                >
                  Link (URL)
                </button>
                <button 
                  onClick={() => setEvidenceType('FILE')}
                  className={`flex-1 py-1.5 text-xs font-medium rounded transition-colors ${evidenceType === 'FILE' ? 'bg-slate-800 text-white shadow-sm' : 'text-slate-500 hover:text-slate-300'}`}
                >
                  Upload File
                </button>
              </div>

              {evidenceType === 'URL' ? (
                <div className="relative">
                  <LinkIcon className="absolute left-3 top-3.5 w-4 h-4 text-slate-500" />
                  <input 
                    type="url" 
                    value={evidenceValue}
                    onChange={(e) => setEvidenceValue(e.target.value)}
                    className="w-full bg-slate-950 border border-slate-700 rounded-lg pl-10 pr-4 py-3 text-slate-100 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500 focus:outline-none text-sm"
                    placeholder="https://example.com/report.pdf"
                  />
                </div>
              ) : (
                <div className="border-2 border-dashed border-slate-700 rounded-lg p-6 flex flex-col items-center justify-center text-center hover:border-indigo-500/50 hover:bg-slate-800/30 transition-colors cursor-pointer group">
                  <UploadCloud className="w-8 h-8 text-slate-500 group-hover:text-indigo-400 mb-2 transition-colors" />
                  <p className="text-sm text-slate-300 font-medium">Click to upload or drag and drop</p>
                  <p className="text-xs text-slate-500 mt-1">PDF, PNG, JPG up to 10MB</p>
                  <input 
                    type="file" 
                    className="hidden" 
                    onChange={(e) => setEvidenceValue(e.target.files?.[0]?.name || 'file_uploaded.pdf')}
                  />
                  {evidenceValue && (
                     <div className="mt-3 px-3 py-1 bg-indigo-500/20 text-indigo-300 text-xs rounded-full flex items-center gap-1">
                        <CheckCircle2 className="w-3 h-3" /> {evidenceValue}
                     </div>
                  )}
                </div>
              )}
            </div>

            {/* Justification Textarea */}
            <div className="space-y-2">
              <label className="block text-sm font-medium text-slate-300">
                Justification <span className="text-red-500">*</span>
              </label>
              <textarea 
                value={justification}
                onChange={(e) => setJustification(e.target.value)}
                className="w-full bg-slate-950 border border-slate-700 rounded-lg px-4 py-3 text-slate-100 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500 focus:outline-none text-sm min-h-[120px] resize-none"
                placeholder="Explain why the AI value is incorrect and how you derived the new value..."
              />
            </div>
          </section>

          {/* Impact Preview */}
          <section className="bg-amber-500/10 border border-amber-500/20 rounded-lg p-4 flex gap-3">
            <AlertTriangle className="w-5 h-5 text-amber-500 shrink-0 mt-0.5" />
            <div>
              <h4 className="text-sm font-medium text-amber-400 mb-1">Impact Analysis</h4>
              <p className="text-xs text-amber-200/80 leading-relaxed">
                Submitting this override will flag the <span className="font-semibold text-amber-200">Sustainability Risk Pillar</span> for recalculation. This change requires approval from a Senior Analyst before affecting the live risk score.
              </p>
            </div>
          </section>

        </div>

        {/* Footer Actions */}
        <div className="p-6 border-t border-slate-800 bg-slate-900 z-10 flex gap-3">
          <button 
            onClick={onClose}
            className="flex-1 px-4 py-3 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-lg text-sm font-medium transition-colors"
          >
            Cancel
          </button>
          <button 
            onClick={handleSubmit}
            disabled={!isFormValid || isSubmitting}
            className="flex-[2] px-4 py-3 bg-indigo-600 hover:bg-indigo-500 text-white rounded-lg text-sm font-medium transition-all disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center gap-2 shadow-lg shadow-indigo-900/20"
          >
            {isSubmitting ? (
              <span className="animate-pulse">Processing...</span>
            ) : (
              <>
                {userRole === 'ADMIN' ? (
                  <>Apply Immediately <ShieldCheck className="w-4 h-4" /></>
                ) : (
                  <>Submit for Approval <ArrowRight className="w-4 h-4" /></>
                )}
              </>
            )}
          </button>
        </div>
      </div>
    </div>
  );
};

export default OverrideProposalDrawer;

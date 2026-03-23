import React, { useState } from 'react';
import { 
  X, 
  UploadCloud, 
  Link as LinkIcon, 
  FileText, 
  CheckCircle2, 
  ArrowRight,
  Tag
} from 'lucide-react';
import { PendingSource } from '../types';

interface AddSourceModalProps {
  isOpen: boolean;
  onClose: () => void;
  onSubmit: (source: PendingSource) => void;
}

const SOURCE_TAGS = [
  'Annual Report',
  'Sustainability Report',
  'Regulatory Filing',
  'Controversy/News',
  'NGO Report',
  'Third-Party Audit',
  'Internal Policy'
];

const AddSourceModal: React.FC<AddSourceModalProps> = ({ isOpen, onClose, onSubmit }) => {
  const [inputType, setInputType] = useState<'URL' | 'FILE'>('URL');
  const [inputValue, setInputValue] = useState('');
  const [selectedTag, setSelectedTag] = useState('');
  const [justification, setJustification] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);

  if (!isOpen) return null;

  const handleSubmit = () => {
    setIsSubmitting(true);
    try {
      onSubmit({
        type: inputType === 'URL' ? 'URL' : 'PDF',
        value: inputValue,
        tags: [selectedTag],
        justification,
        submittedAt: new Date().toISOString(),
        submittedBy: 'Current User'
      });
      // Reset form
      setInputValue('');
      setSelectedTag('');
      setJustification('');
      onClose();
    } finally {
      setIsSubmitting(false);
    }
  };

  const isFormValid = inputValue && selectedTag && justification.length > 10;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      {/* Backdrop */}
      <div 
        className="absolute inset-0 bg-slate-950/80 backdrop-blur-sm transition-opacity"
        onClick={onClose}
      />

      {/* Modal Content */}
      <div className="relative w-full max-w-lg bg-slate-900 border border-slate-800 rounded-xl shadow-2xl overflow-hidden flex flex-col max-h-[90vh]">
        
        {/* Header */}
        <div className="h-16 flex items-center justify-between px-6 border-b border-slate-800 bg-slate-900/50">
          <div>
            <h2 className="text-lg font-semibold text-slate-100">Propose New Data Source</h2>
            <p className="text-xs text-slate-500">Feed the Beast</p>
          </div>
          <button 
            onClick={onClose}
            className="p-2 hover:bg-slate-800 rounded-full text-slate-400 hover:text-white transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Body */}
        <div className="p-6 space-y-6 overflow-y-auto">
          
          {/* Input Type Toggle */}
          <div className="flex bg-slate-950 rounded-lg p-1 border border-slate-700">
            <button 
              onClick={() => setInputType('URL')}
              className={`flex-1 py-2 text-sm font-medium rounded-md transition-all flex items-center justify-center gap-2 ${inputType === 'URL' ? 'bg-slate-800 text-white shadow-sm' : 'text-slate-500 hover:text-slate-300'}`}
            >
              <LinkIcon className="w-4 h-4" /> URL / Webpage
            </button>
            <button 
              onClick={() => setInputType('FILE')}
              className={`flex-1 py-2 text-sm font-medium rounded-md transition-all flex items-center justify-center gap-2 ${inputType === 'FILE' ? 'bg-slate-800 text-white shadow-sm' : 'text-slate-500 hover:text-slate-300'}`}
            >
              <FileText className="w-4 h-4" /> Document Upload
            </button>
          </div>

          {/* Main Input */}
          <div className="space-y-2">
            <label className="block text-sm font-medium text-slate-300">
              {inputType === 'URL' ? 'Source URL' : 'Upload Document'} <span className="text-red-500">*</span>
            </label>
            
            {inputType === 'URL' ? (
              <div className="relative">
                <LinkIcon className="absolute left-3 top-3.5 w-4 h-4 text-slate-500" />
                <input 
                  type="url" 
                  value={inputValue}
                  onChange={(e) => setInputValue(e.target.value)}
                  className="w-full bg-slate-950 border border-slate-700 rounded-lg pl-10 pr-4 py-3 text-slate-100 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500 focus:outline-none text-sm font-mono"
                  placeholder="https://example.com/report.pdf"
                  autoFocus
                />
              </div>
            ) : (
              <div className="border-2 border-dashed border-slate-700 rounded-lg p-8 flex flex-col items-center justify-center text-center hover:border-indigo-500/50 hover:bg-slate-800/30 transition-colors cursor-pointer group relative">
                <input 
                    type="file" 
                    className="absolute inset-0 opacity-0 cursor-pointer" 
                    onChange={(e) => setInputValue(e.target.files?.[0]?.name || 'uploaded_file.pdf')}
                />
                <UploadCloud className="w-10 h-10 text-slate-500 group-hover:text-indigo-400 mb-3 transition-colors" />
                <p className="text-sm text-slate-300 font-medium">Click to upload or drag and drop</p>
                <p className="text-xs text-slate-500 mt-1">PDF, CSV, XLSX up to 25MB</p>
                {inputValue && (
                   <div className="mt-4 px-3 py-1.5 bg-indigo-500/20 text-indigo-300 text-xs rounded-full flex items-center gap-1 font-mono">
                      <CheckCircle2 className="w-3.5 h-3.5" /> {inputValue}
                   </div>
                )}
              </div>
            )}
          </div>

          {/* Context Tagging */}
          <div className="space-y-2">
            <label className="block text-sm font-medium text-slate-300">
              Source Category <span className="text-red-500">*</span>
            </label>
            <div className="relative">
              <Tag className="absolute left-3 top-3.5 w-4 h-4 text-slate-500" />
              <select 
                value={selectedTag}
                onChange={(e) => setSelectedTag(e.target.value)}
                className="w-full bg-slate-950 border border-slate-700 rounded-lg pl-10 pr-4 py-3 text-slate-100 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500 focus:outline-none text-sm appearance-none cursor-pointer"
              >
                <option value="" disabled>Select a category...</option>
                {SOURCE_TAGS.map(tag => (
                  <option key={tag} value={tag}>{tag}</option>
                ))}
              </select>
            </div>
          </div>

          {/* Justification */}
          <div className="space-y-2">
            <label className="block text-sm font-medium text-slate-300">
              Justification <span className="text-red-500">*</span>
            </label>
            <textarea 
              value={justification}
              onChange={(e) => setJustification(e.target.value)}
              className="w-full bg-slate-950 border border-slate-700 rounded-lg px-4 py-3 text-slate-100 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500 focus:outline-none text-sm min-h-[100px] resize-none"
              placeholder="Why is this source needed? What information is the AI currently missing?"
            />
          </div>

        </div>

        {/* Footer */}
        <div className="p-6 border-t border-slate-800 bg-slate-900/50 flex gap-3">
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
              <span className="animate-pulse">Submitting...</span>
            ) : (
              <>
                Submit Source for Approval <ArrowRight className="w-4 h-4" />
              </>
            )}
          </button>
        </div>

      </div>
    </div>
  );
};

export default AddSourceModal;

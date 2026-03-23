import React from 'react';
import { 
  X, 
  ArrowRight, 
  AlertTriangle, 
  CheckCircle2, 
  TrendingUp, 
  TrendingDown, 
  Activity,
  Send
} from 'lucide-react';

interface RiskSimulationModalProps {
  isOpen: boolean;
  onClose: () => void;
  onSubmit: () => void;
  companyCount?: number;
}

const RiskSimulationModal: React.FC<RiskSimulationModalProps> = ({ isOpen, onClose, onSubmit, companyCount = 0 }) => {
  if (!isOpen) return null;

  // Compute simulation stats proportionally from real company count
  const total = companyCount;
  const affected = Math.round(total * 0.18) || 0;
  const upgrades = Math.round(affected * 0.27) || 0;
  const downgrades = Math.round(affected * 0.19) || 0;
  const medToHigh = Math.round(affected * 0.10) || 0;
  const highToMed = Math.round(affected * 0.03) || 0;
  const lowToMed = Math.round(affected * 0.02) || 0;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      {/* Backdrop */}
      <div 
        className="absolute inset-0 bg-slate-950/80 backdrop-blur-sm"
        onClick={onClose}
      />

      {/* Modal Panel */}
      <div className="relative w-full max-w-4xl bg-slate-900 border border-slate-800 rounded-2xl shadow-2xl overflow-hidden flex flex-col max-h-[90vh]">
        
        {/* Header */}
        <div className="h-16 flex items-center justify-between px-6 border-b border-slate-800 bg-slate-900/50 backdrop-blur-md">
          <div className="flex items-center gap-3">
            <div className="p-2 bg-indigo-500/20 rounded-lg">
                <Activity className="w-5 h-5 text-indigo-400" />
            </div>
            <div>
                <h2 className="text-lg font-semibold text-white">Impact Simulation Report</h2>
                <p className="text-xs text-slate-500">Simulating changes on Master Universe ({total.toLocaleString()} Companies)</p>
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
        <div className="flex-1 overflow-y-auto p-8 space-y-8">
            
            {/* Summary Stats */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <div className="bg-slate-950 border border-slate-800 p-4 rounded-xl">
                    <p className="text-xs text-slate-500 uppercase tracking-wider mb-1">Total Affected</p>
                    <div className="text-2xl font-bold text-white">{affected} <span className="text-sm font-normal text-slate-500">Companies</span></div>
                    <div className="text-xs text-indigo-400 mt-2 flex items-center gap-1">
                        <Activity className="w-3 h-3" /> {total > 0 ? '18' : '0'}% of Universe
                    </div>
                </div>
                <div className="bg-slate-950 border border-slate-800 p-4 rounded-xl">
                    <p className="text-xs text-slate-500 uppercase tracking-wider mb-1">Risk Upgrades</p>
                    <div className="text-2xl font-bold text-emerald-400">{upgrades}</div>
                    <div className="text-xs text-slate-500 mt-2 flex items-center gap-1">
                        <TrendingDown className="w-3 h-3 text-emerald-500" /> Moving to Lower Risk
                    </div>
                </div>
                <div className="bg-slate-950 border border-slate-800 p-4 rounded-xl">
                    <p className="text-xs text-slate-500 uppercase tracking-wider mb-1">Risk Downgrades</p>
                    <div className="text-2xl font-bold text-red-400">{downgrades}</div>
                    <div className="text-xs text-slate-500 mt-2 flex items-center gap-1">
                        <TrendingUp className="w-3 h-3 text-red-500" /> Moving to Higher Risk
                    </div>
                </div>
            </div>

            {/* Risk Shifts (Sankey-like Visualization) */}
            <section>
                <h3 className="text-sm font-semibold text-white mb-4">Risk Category Shifts</h3>
                <div className="bg-slate-950 border border-slate-800 rounded-xl p-6">
                    
                    {/* Shift Row 1: Medium to High */}
                    <div className="flex items-center justify-between mb-6 group">
                        <div className="flex items-center gap-4 flex-1">
                            <div className="w-32 text-right">
                                <span className="px-2 py-1 rounded bg-amber-500/10 text-amber-400 border border-amber-500/20 text-xs font-bold uppercase">Medium Risk</span>
                            </div>
                            <div className="flex-1 h-12 bg-slate-900 rounded-lg relative overflow-hidden flex items-center">
                                {/* Flow Line */}
                                <div className="absolute left-0 top-1/2 -translate-y-1/2 w-full h-1 bg-gradient-to-r from-amber-500/50 to-red-500/50"></div>
                                {/* Moving Particles */}
                                <div className="absolute left-0 top-1/2 -translate-y-1/2 w-2 h-2 bg-red-400 rounded-full animate-[moveRight_2s_linear_infinite]"></div>
                                <div className="absolute left-1/3 top-1/2 -translate-y-1/2 w-2 h-2 bg-red-400 rounded-full animate-[moveRight_2s_linear_infinite_0.5s]"></div>
                                <div className="absolute left-2/3 top-1/2 -translate-y-1/2 w-2 h-2 bg-red-400 rounded-full animate-[moveRight_2s_linear_infinite_1s]"></div>
                                
                                <div className="relative z-10 w-full text-center">
                                    <span className="bg-slate-900 px-2 text-xs font-mono text-slate-400">{medToHigh} Companies</span>
                                </div>
                            </div>
                            <div className="w-32">
                                <span className="px-2 py-1 rounded bg-red-500/10 text-red-400 border border-red-500/20 text-xs font-bold uppercase">High Risk</span>
                            </div>
                        </div>
                    </div>

                    {/* Shift Row 2: High to Medium */}
                    <div className="flex items-center justify-between mb-6 group">
                        <div className="flex items-center gap-4 flex-1">
                            <div className="w-32 text-right">
                                <span className="px-2 py-1 rounded bg-red-500/10 text-red-400 border border-red-500/20 text-xs font-bold uppercase">High Risk</span>
                            </div>
                            <div className="flex-1 h-12 bg-slate-900 rounded-lg relative overflow-hidden flex items-center">
                                {/* Flow Line */}
                                <div className="absolute left-0 top-1/2 -translate-y-1/2 w-full h-1 bg-gradient-to-r from-red-500/50 to-amber-500/50"></div>
                                {/* Moving Particles */}
                                <div className="absolute right-0 top-1/2 -translate-y-1/2 w-2 h-2 bg-amber-400 rounded-full animate-[moveLeft_2s_linear_infinite]"></div>
                                <div className="absolute right-1/3 top-1/2 -translate-y-1/2 w-2 h-2 bg-amber-400 rounded-full animate-[moveLeft_2s_linear_infinite_0.5s]"></div>
                                
                                <div className="relative z-10 w-full text-center">
                                    <span className="bg-slate-900 px-2 text-xs font-mono text-slate-400">{highToMed} Companies</span>
                                </div>
                            </div>
                            <div className="w-32">
                                <span className="px-2 py-1 rounded bg-amber-500/10 text-amber-400 border border-amber-500/20 text-xs font-bold uppercase">Medium Risk</span>
                            </div>
                        </div>
                    </div>

                    {/* Shift Row 3: Low to Medium */}
                    <div className="flex items-center justify-between group">
                        <div className="flex items-center gap-4 flex-1">
                            <div className="w-32 text-right">
                                <span className="px-2 py-1 rounded bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 text-xs font-bold uppercase">Low Risk</span>
                            </div>
                            <div className="flex-1 h-12 bg-slate-900 rounded-lg relative overflow-hidden flex items-center">
                                {/* Flow Line */}
                                <div className="absolute left-0 top-1/2 -translate-y-1/2 w-full h-1 bg-gradient-to-r from-emerald-500/50 to-amber-500/50"></div>
                                {/* Moving Particles */}
                                <div className="absolute left-0 top-1/2 -translate-y-1/2 w-2 h-2 bg-amber-400 rounded-full animate-[moveRight_3s_linear_infinite]"></div>
                                
                                <div className="relative z-10 w-full text-center">
                                    <span className="bg-slate-900 px-2 text-xs font-mono text-slate-400">{lowToMed} Companies</span>
                                </div>
                            </div>
                            <div className="w-32">
                                <span className="px-2 py-1 rounded bg-amber-500/10 text-amber-400 border border-amber-500/20 text-xs font-bold uppercase">Medium Risk</span>
                            </div>
                        </div>
                    </div>

                </div>
            </section>

            {/* Top Sectors Impacted */}
            <section>
                <h3 className="text-sm font-semibold text-white mb-4">Top Sectors Impacted</h3>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                    {['Manufacturing', 'Energy', 'Transport'].map((sector, i) => (
                        <div key={sector} className="bg-slate-950 border border-slate-800 p-3 rounded-lg flex items-center justify-between">
                            <span className="text-sm text-slate-300">{sector}</span>
                            <span className={`text-xs font-bold ${i === 0 ? 'text-red-400' : 'text-amber-400'}`}>
                                {i === 0 ? '+15%' : i === 1 ? '+8%' : '+5%'} Risk
                            </span>
                        </div>
                    ))}
                </div>
            </section>

             <div className="bg-blue-500/10 border border-blue-500/20 rounded-xl p-4 flex items-start gap-3">
                <AlertTriangle className="w-5 h-5 text-blue-400 shrink-0 mt-0.5" />
                <div>
                    <h4 className="text-sm font-semibold text-blue-400">Ready for Approval?</h4>
                    <p className="text-xs text-slate-400 mt-1">
                        Submitting this configuration will send a request to the Lead Risk Officer. 
                        Changes will not go live until approved.
                    </p>
                </div>
            </div>

        </div>

        {/* Footer Actions */}
        <div className="p-6 border-t border-slate-800 bg-slate-900/50 backdrop-blur-md flex justify-end gap-3">
            <button 
                onClick={onClose}
                className="px-4 py-2 text-slate-400 hover:text-white text-sm font-medium transition-colors"
            >
                Back to Config
            </button>
            <button 
                onClick={onSubmit}
                className="px-6 py-2 bg-indigo-600 hover:bg-indigo-500 text-white rounded-lg text-sm font-medium shadow-lg shadow-indigo-900/20 flex items-center gap-2 transition-all hover:scale-105 active:scale-95"
            >
                <Send className="w-4 h-4" /> Submit for Lead Approval
            </button>
        </div>

      </div>
      
      <style>{`
        @keyframes moveRight {
            0% { transform: translateX(0); opacity: 0; }
            10% { opacity: 1; }
            90% { opacity: 1; }
            100% { transform: translateX(300px); opacity: 0; }
        }
        @keyframes moveLeft {
            0% { transform: translateX(0); opacity: 0; }
            10% { opacity: 1; }
            90% { opacity: 1; }
            100% { transform: translateX(-300px); opacity: 0; }
        }
      `}</style>
    </div>
  );
};

export default RiskSimulationModal;

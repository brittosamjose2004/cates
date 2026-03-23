import React from 'react';
import { CompanyData, Indicator } from '../types';
import { AlertCircle, CheckCircle2, Edit2, ShieldCheck, FileText, Clock } from 'lucide-react';

interface IndicatorTableProps {
  company: CompanyData;
  onOverrideClick: (indicator: Indicator) => void;
  onLineageClick: (indicator: Indicator) => void;
}

const IndicatorTable: React.FC<IndicatorTableProps> = ({ company, onOverrideClick, onLineageClick }) => {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-left border-collapse">
        <thead>
          <tr className="border-b border-slate-700 text-xs text-slate-500 uppercase tracking-wider">
            <th className="py-3 px-4 font-semibold">Indicator</th>
            <th className="py-3 px-4 font-semibold">Value</th>
            <th className="py-3 px-4 font-semibold">Confidence</th>
            <th className="py-3 px-4 font-semibold">Source</th>
            <th className="py-3 px-4 font-semibold text-right">Last Updated</th>
            <th className="py-3 px-4 font-semibold w-16"></th>
          </tr>
        </thead>
        <tbody className="text-sm">
          {company.indicators.map((indicator) => (
            <tr 
              key={indicator.id} 
              className={`
                group border-b border-slate-800 transition-colors hover:bg-slate-800/30
                ${indicator.pendingOverride ? 'bg-amber-500/5' : ''}
              `}
            >
              {/* Name */}
              <td className="py-3 px-4">
                <div className="flex items-center gap-2">
                  <span className="text-slate-200 font-medium">{indicator.name}</span>
                  {indicator.isOverridden && !indicator.pendingOverride && (
                    <div className="group/tooltip relative">
                       <ShieldCheck className="w-4 h-4 text-emerald-500 cursor-help" />
                       <div className="absolute left-0 bottom-6 w-48 bg-slate-900 border border-slate-700 p-2 rounded shadow-xl hidden group-hover/tooltip:block z-10">
                          <p className="text-xs text-slate-400">Manually Verified</p>
                          <p className="text-xs text-slate-500 italic mt-1">"{indicator.overrideReason}"</p>
                       </div>
                    </div>
                  )}
                  {indicator.pendingOverride && (
                    <div className="group/tooltip relative">
                       <Clock className="w-4 h-4 text-amber-500 cursor-help animate-pulse" />
                       <div className="absolute left-0 bottom-6 w-64 bg-slate-900 border border-amber-500/30 p-3 rounded shadow-xl hidden group-hover/tooltip:block z-10">
                          <p className="text-xs font-semibold text-amber-400 mb-1">Pending Approval</p>
                          <p className="text-xs text-slate-400">
                            Proposed: <span className="text-slate-200 font-mono">{indicator.pendingOverride.newValue}</span>
                          </p>
                          <p className="text-xs text-slate-500 italic mt-1 truncate">"{indicator.pendingOverride.justification}"</p>
                       </div>
                    </div>
                  )}
                </div>
              </td>

              {/* Value - Clickable for Lineage */}
              <td className="py-3 px-4">
                  <button 
                    onClick={() => onLineageClick(indicator)}
                    className="text-left group/val relative"
                  >
                    <span className={`font-mono ${indicator.isOverridden ? 'text-emerald-400 font-bold' : 'text-slate-300'} border-b border-dashed border-slate-600 hover:border-indigo-400 transition-colors`}>
                        {indicator.value} <span className="text-slate-500 text-xs">{indicator.unit}</span>
                    </span>
                    <span className="absolute -top-8 left-0 bg-slate-800 text-xs px-2 py-1 rounded opacity-0 group-hover/val:opacity-100 transition-opacity whitespace-nowrap border border-slate-700 pointer-events-none z-50 shadow-lg">
                        View Lineage
                    </span>
                  </button>
              </td>

              {/* Confidence Badge */}
              <td className="py-3 px-4">
                <div className="flex items-center gap-2">
                  <div className="w-16 h-1.5 bg-slate-800 rounded-full overflow-hidden">
                    <div 
                      className={`h-full rounded-full ${indicator.confidence > 90 ? 'bg-emerald-500' : indicator.confidence > 70 ? 'bg-amber-500' : 'bg-red-500'}`}
                      style={{ width: `${indicator.confidence}%` }}
                    ></div>
                  </div>
                  <span className="text-xs text-slate-500">{indicator.confidence}%</span>
                </div>
              </td>

              {/* Source */}
              <td className="py-3 px-4">
                 <div className="flex items-center gap-1.5 text-slate-400">
                    <FileText className="w-3.5 h-3.5" />
                    <span className="truncate max-w-[150px]">{indicator.source}</span>
                 </div>
              </td>

              {/* Last Updated */}
              <td className="py-3 px-4 text-right text-slate-500 font-mono text-xs">
                  {indicator.lastUpdated}
              </td>

              {/* Actions */}
              <td className="py-3 px-4 text-right">
                  <button 
                      onClick={() => onOverrideClick(indicator)}
                      disabled={!!indicator.pendingOverride}
                      className={`
                        p-1.5 rounded transition-all
                        ${indicator.pendingOverride 
                          ? 'opacity-50 cursor-not-allowed text-slate-600' 
                          : 'opacity-0 group-hover:opacity-100 hover:bg-slate-700 text-slate-400 hover:text-indigo-400'
                        }
                      `}
                      title={indicator.pendingOverride ? "Override pending approval" : "Propose Override"}
                  >
                      <Edit2 className="w-4 h-4" />
                  </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

export default IndicatorTable;
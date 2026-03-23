import React from 'react';
import { RiskPillar } from '../../types';
import { ArrowUpRight, ArrowDownRight, Minus } from 'lucide-react';

interface RiskCardProps {
  pillar: RiskPillar;
}

const RiskCard: React.FC<RiskCardProps> = ({ pillar }) => {
  // Traffic Light Logic
  // High Score = High Risk = BAD (Red)
  let statusColor = 'text-emerald-500';
  let bgColor = 'bg-emerald-500/10';
  let borderColor = 'border-emerald-500/20';
  let progressColor = 'bg-emerald-500';

  if (pillar.score >= 75) {
    statusColor = 'text-red-500';
    bgColor = 'bg-red-500/10';
    borderColor = 'border-red-500/20';
    progressColor = 'bg-red-500';
  } else if (pillar.score >= 45) {
    statusColor = 'text-amber-500';
    bgColor = 'bg-amber-500/10';
    borderColor = 'border-amber-500/20';
    progressColor = 'bg-amber-500';
  }

  return (
    <div className={`relative p-5 rounded-lg border ${borderColor} ${bgColor} backdrop-blur-sm transition-all hover:shadow-lg hover:shadow-black/20`}>
      <div className="flex justify-between items-start mb-4">
        <div>
          <h3 className="text-slate-400 text-sm font-medium uppercase tracking-wider">{pillar.name}</h3>
          <div className="flex items-baseline mt-1 gap-2">
            <span className={`text-4xl font-bold ${statusColor}`}>{pillar.score}</span>
            <span className="text-slate-500 text-xs font-mono">/100</span>
          </div>
        </div>
        
        {/* Trend Indicator */}
        <div className={`flex items-center gap-1 text-xs font-medium px-2 py-1 rounded-full bg-slate-800/50 border border-slate-700 ${
          pillar.trend === 'up' ? 'text-red-400' : pillar.trend === 'down' ? 'text-emerald-400' : 'text-slate-400'
        }`}>
          {pillar.trend === 'up' && <ArrowUpRight className="w-3 h-3" />}
          {pillar.trend === 'down' && <ArrowDownRight className="w-3 h-3" />}
          {pillar.trend === 'stable' && <Minus className="w-3 h-3" />}
          {Math.abs(pillar.trendValue)}%
        </div>
      </div>

      {/* Progress Bar (Gauge equivalent) */}
      <div className="w-full h-1.5 bg-slate-800 rounded-full overflow-hidden mb-4">
        <div 
          className={`h-full rounded-full ${progressColor} transition-all duration-1000 ease-out`} 
          style={{ width: `${pillar.score}%` }}
        />
      </div>

      {/* Top Drivers */}
      <div>
        <h4 className="text-xs text-slate-500 uppercase font-semibold mb-2">Top Drivers</h4>
        <div className="space-y-2">
          {pillar.drivers.map((driver) => (
            <div key={driver.id} className="flex justify-between items-center text-sm">
              <span className="text-slate-300 truncate pr-2">{driver.name}</span>
              <div className="flex items-center gap-2">
                <div className="w-16 h-1 bg-slate-800 rounded-full overflow-hidden">
                   <div className="h-full bg-slate-500" style={{ width: `${driver.impact}%` }}></div>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

export default RiskCard;
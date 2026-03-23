import React, { useState, useEffect } from 'react';
import { CompanySummary, PipelineStatus, UserRole } from '../types';
import { Search, Filter, MoreHorizontal, Plus, RefreshCw, LayoutGrid, List, AlertCircle, Play } from 'lucide-react';
import AddCompanyModal from './AddCompanyModal';
import GlobalRunPipelineModal from './GlobalRunPipelineModal';
import api, { PipelineJob } from '../apiService';

interface MasterUniverseProps {
  onNavigateToCompany: (id: string) => void;
  userRole: UserRole;
}

const MasterUniverse: React.FC<MasterUniverseProps> = ({ onNavigateToCompany, userRole }) => {
  const [companies, setCompanies] = useState<CompanySummary[]>([]);
  const [isAddModalOpen, setIsAddModalOpen] = useState(false);
  const [isRunModalOpen, setIsRunModalOpen] = useState(false);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [isLoading, setIsLoading] = useState(true);
  const [activeFilter, setActiveFilter] = useState<'ALL' | 'ERRORS' | 'PUBLISHED'>('ALL');
  const [searchQuery, setSearchQuery] = useState('');

  // Load companies from the real API
  useEffect(() => {
    loadCompanies();
  }, []);

  const loadCompanies = async () => {
    setIsLoading(true);
    try {
      const data = await api.getCompanies();
      setCompanies(data);
    } catch (err) {
      console.error('Failed to load companies:', err);
    } finally {
      setIsLoading(false);
    }
  };
  // Filter Logic
  const filteredCompanies = companies.filter(c => {
    // Status Filter
    if (activeFilter === 'ERRORS' && !(c.status === 'ERROR' || c.status === 'NEEDS_REVIEW')) return false;
    if (activeFilter === 'PUBLISHED' && c.status !== 'PUBLISHED') return false;
    
    // Search Filter
    if (searchQuery) {
      const query = searchQuery.toLowerCase();
      return (
        c.name.toLowerCase().includes(query) ||
        c.ticker.toLowerCase().includes(query) ||
        c.lei.toLowerCase().includes(query)
      );
    }
    
    return true;
  });

  const handleStartPipeline = async (config: { dataSources: string[]; financialYears: string[]; companyIds: string[]; allYears: boolean }): Promise<PipelineJob[]> => {
    setIsRefreshing(true);
    try {
      const jobs = await api.runPipeline({
        company_ids: config.companyIds,
        data_sources: config.dataSources,
        financial_years: config.financialYears,
        all_years: config.allYears,
      });
      return jobs;
    } catch (err) {
      console.error('Pipeline start failed:', err);
      setIsRefreshing(false);
      throw err;
    }
  };

  const handlePipelineComplete = async () => {
    await loadCompanies();
    setIsRefreshing(false);
  };

  const handleAddCompany = (created: any) => {
    // API call already done inside AddCompanyModal — just update local state
    setCompanies(prev => [created, ...prev]);
    setIsAddModalOpen(false);
  };

  return (
    <div className="h-full flex flex-col bg-slate-900">
      {/* Header */}
      <div className="px-8 py-6 flex items-end justify-between border-b border-slate-800 bg-slate-900/80 backdrop-blur z-10">
        <div>
           <h1 className="text-2xl font-bold text-white tracking-tight">Master Universe</h1>
           <p className="text-slate-500 text-sm mt-1">Manage entity ingestion pipelines and risk score publishing.</p>
        </div>
        <div className="flex items-center gap-3">
          <button 
             onClick={() => setIsRunModalOpen(true)}
             className="bg-slate-800 hover:bg-slate-700 text-slate-300 px-4 py-2.5 rounded-lg text-sm font-medium flex items-center gap-2 border border-slate-700 transition-all"
          >
              <Play className="w-4 h-4 fill-current" /> Run Pipeline
          </button>
          <button 
             onClick={() => setIsAddModalOpen(true)}
             className="bg-indigo-600 hover:bg-indigo-500 text-white px-4 py-2.5 rounded-lg text-sm font-medium flex items-center gap-2 shadow-lg shadow-indigo-500/20 transition-all hover:scale-105"
          >
              <Plus className="w-4 h-4" /> Add Company
          </button>
        </div>
      </div>

      {/* Toolbar */}
      <div className="px-8 py-4 flex items-center justify-between gap-4">
         <div className="flex items-center gap-2 bg-slate-800 p-1 rounded-lg border border-slate-700">
            <button 
                onClick={() => setActiveFilter('ALL')}
                className={`px-3 py-1.5 rounded-md text-xs font-medium transition-colors ${activeFilter === 'ALL' ? 'bg-slate-600 text-white' : 'text-slate-400 hover:text-slate-200'}`}
            >
                All Companies
            </button>
            <button 
                onClick={() => setActiveFilter('PUBLISHED')}
                className={`px-3 py-1.5 rounded-md text-xs font-medium transition-colors ${activeFilter === 'PUBLISHED' ? 'bg-emerald-500/20 text-emerald-400' : 'text-slate-400 hover:text-slate-200'}`}
            >
                Published
            </button>
            <button 
                onClick={() => setActiveFilter('ERRORS')}
                className={`px-3 py-1.5 rounded-md text-xs font-medium transition-colors ${activeFilter === 'ERRORS' ? 'bg-red-500/20 text-red-400' : 'text-slate-400 hover:text-slate-200'}`}
            >
                Errors & Review
            </button>
         </div>

        <div className="flex items-center gap-3">
            <div className="relative group">
                <Search className="absolute left-3 top-2.5 w-4 h-4 text-slate-500 group-focus-within:text-indigo-500" />
                <input 
                    type="text" 
                    placeholder="Search by name, ticker, LEI..." 
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    className="bg-slate-800 border border-slate-700 rounded-lg pl-9 pr-4 py-2 text-sm text-slate-200 focus:border-indigo-500 focus:outline-none w-64 transition-all"
                />
            </div>
            <button className="p-2 text-slate-400 hover:bg-slate-800 rounded-lg border border-transparent hover:border-slate-700">
                <Filter className="w-4 h-4" />
            </button>
         </div>
      </div>

      {/* High Density Grid */}
      <div className="flex-1 overflow-hidden px-8 pb-8 relative">
         {isRefreshing && (
           <div className="absolute inset-0 bg-slate-900/40 backdrop-blur-[1px] z-20 flex items-center justify-center rounded-xl mx-8 mb-8">
             <RefreshCw className="w-10 h-10 text-indigo-500 animate-spin" />
           </div>
         )}
         <div className="h-full overflow-y-auto border border-slate-800 rounded-lg bg-slate-900 shadow-xl">
             <table className="w-full text-left border-collapse">
                 <thead className="bg-slate-950 sticky top-0 z-10 shadow-sm">
                     <tr className="text-xs text-slate-500 uppercase tracking-wider">
                         <th className="py-4 px-6 font-semibold w-[35%]">Company</th>
                         <th className="py-4 px-6 font-semibold">Pipeline Status</th>
                         <th className="py-4 px-6 font-semibold">Risk Summary</th>
                         <th className="py-4 px-6 font-semibold">Last Updated</th>
                         <th className="py-4 px-6 font-semibold text-right">Actions</th>
                     </tr>
                 </thead>
                 <tbody className="divide-y divide-slate-800">
                     {isLoading ? (
                       <tr><td colSpan={5} className="py-12 text-center text-slate-500">
                         <RefreshCw className="w-6 h-6 animate-spin mx-auto mb-2" />Loading companies...
                       </td></tr>
                     ) : filteredCompanies.length === 0 ? (
                       <tr><td colSpan={5} className="py-12 text-center text-slate-500">
                         No companies found.
                       </td></tr>
                     ) : filteredCompanies.map((company) => (
                         <tr 
                            key={company.id} 
                            onClick={() => onNavigateToCompany(company.id)}
                            className="group hover:bg-slate-800/40 transition-colors cursor-pointer"
                         >
                             {/* Company Info */}
                             <td className="py-3 px-6">
                                 <div className="flex items-center gap-3">
                                     <div className="w-10 h-10 rounded-full bg-slate-800 flex items-center justify-center text-slate-300 font-bold border border-slate-700 group-hover:border-indigo-500/50 transition-colors">
                                         {company.name.charAt(0)}
                                     </div>
                                     <div>
                                         <h3 className="text-slate-200 font-medium text-sm group-hover:text-indigo-400 transition-colors">{company.name}</h3>
                                         <div className="flex items-center gap-2 mt-0.5">
                                             <span className="text-[10px] font-mono bg-slate-800 px-1.5 py-0.5 rounded text-slate-400 border border-slate-700">{company.ticker}</span>
                                             {/* Region Flag Sim */}
                                             <span className="text-[10px] text-slate-500">{company.region} • {company.sector}</span>
                                         </div>
                                     </div>
                                 </div>
                             </td>

                             {/* Status Badge */}
                             <td className="py-3 px-6">
                                 <StatusBadge status={company.status} />
                             </td>

                             {/* Risk Sparklines */}
                             <td className="py-3 px-6">
                                 {company.status === 'PUBLISHED' ? (
                                     <div className="flex items-end gap-1 h-8 w-24">
                                         <RiskBar score={company.riskScores.s} label="S" />
                                         <RiskBar score={company.riskScores.p} label="P" />
                                         <RiskBar score={company.riskScores.o} label="O" />
                                         <RiskBar score={company.riskScores.f} label="F" />
                                     </div>
                                 ) : (
                                     <div className="h-1 w-24 bg-slate-800 rounded overflow-hidden">
                                        <div className="h-full bg-indigo-500/20 w-1/2 animate-pulse"></div>
                                     </div>
                                 )}
                             </td>

                             {/* Last Updated */}
                             <td className="py-3 px-6">
                                 <div className="text-sm text-slate-400 flex items-center gap-2">
                                     {company.lastUpdated}
                                 </div>
                             </td>

                             {/* Actions */}
                             <td className="py-3 px-6 text-right">
                                 <button className="p-1.5 text-slate-500 hover:text-white hover:bg-slate-700 rounded transition-all opacity-0 group-hover:opacity-100" onClick={(e) => e.stopPropagation()}>
                                     <MoreHorizontal className="w-4 h-4" />
                                 </button>
                             </td>
                         </tr>
                     ))}
                 </tbody>
             </table>
         </div>
      </div>

      <AddCompanyModal 
        isOpen={isAddModalOpen} 
        onClose={() => setIsAddModalOpen(false)} 
        onAdd={handleAddCompany}
      />

      <GlobalRunPipelineModal 
        isOpen={isRunModalOpen}
        onClose={() => setIsRunModalOpen(false)}
        onStart={handleStartPipeline}
        onJobsComplete={handlePipelineComplete}
        companies={companies}
      />
    </div>
  );
};

// Sub-components for Cell Rendering

const StatusBadge = ({ status }: { status: PipelineStatus }) => {
    switch (status) {
        case 'PUBLISHED':
            return <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
                <span className="w-1.5 h-1.5 rounded-full bg-emerald-500"></span> Published
            </span>;
        case 'FETCHING':
            return <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium bg-indigo-500/10 text-indigo-400 border border-indigo-500/20">
                 <span className="w-1.5 h-1.5 rounded-full bg-indigo-400 animate-pulse"></span> Fetching
            </span>;
        case 'SCORING':
            return <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium bg-blue-500/10 text-blue-400 border border-blue-500/20">
                <span className="w-1.5 h-1.5 rounded-full bg-blue-400 animate-pulse"></span> Scoring
            </span>;
        case 'QUEUED':
            return <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium bg-slate-800 text-slate-400 border border-slate-700">
                Queued
            </span>;
        case 'ERROR':
        case 'NEEDS_REVIEW':
            return <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium bg-red-500/10 text-red-400 border border-red-500/20">
                <AlertCircle className="w-3 h-3" /> Action Reqd
            </span>;
        default:
            return null;
    }
}

const RiskBar = ({ score, label }: { score: number, label: string }) => {
    // Height calculation (min 10% for visibility)
    const height = Math.max(10, score);
    
    let colorClass = 'bg-emerald-500';
    if (score >= 75) colorClass = 'bg-red-500';
    else if (score >= 45) colorClass = 'bg-amber-500';
    else if (score === 0) colorClass = 'bg-slate-700';

    return (
        <div className="flex flex-col items-center gap-1 flex-1 h-full justify-end group/bar relative">
            <div 
                className={`w-full rounded-sm ${colorClass} opacity-80 group-hover/bar:opacity-100 transition-all`} 
                style={{ height: `${height}%` }}
            ></div>
            {/* Tooltip */}
             <div className="absolute bottom-full mb-1 opacity-0 group-hover/bar:opacity-100 bg-slate-800 text-[10px] text-white px-1.5 py-0.5 rounded border border-slate-700 pointer-events-none whitespace-nowrap z-20">
                {label}: {score}
             </div>
        </div>
    );
}

export default MasterUniverse;
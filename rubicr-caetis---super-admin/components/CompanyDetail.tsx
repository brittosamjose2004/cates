import React, { useState, useEffect } from 'react';
import { CompanyData, Indicator, PendingOverride, UserRole } from '../types';
import { 
  Search, 
  Bell, 
  ChevronRight, 
  GitBranch, 
  Play, 
  RefreshCw,
  ArrowLeft,
  CheckCircle,
  Clock
} from 'lucide-react';
import RiskCard from './ui/RiskCard';
import EvidencePanel from './EvidencePanel';
import IndicatorTable from './IndicatorTable';
import OverrideProposalDrawer from './OverrideProposalDrawer';
import LineageDrawer from './LineageDrawer';
import RunPipelineModal from './RunPipelineModal';
import InterventionDrawer from './InterventionDrawer';
import api, { PipelineJob } from '../apiService';

interface CompanyDetailProps {
    onBack: () => void;
    userRole: UserRole;
    companyId?: string;
}

const CompanyDetail: React.FC<CompanyDetailProps> = ({ onBack, userRole, companyId }) => {
  const [activeTab, setActiveTab] = useState<'indicators' | 'evidence'>('indicators');
  const [mode, setMode] = useState<'DRAFT' | 'LIVE'>('LIVE');
  const [selectedFY, setSelectedFY] = useState<string>('');
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [isLoading, setIsLoading] = useState(true);
  const [availableYears, setAvailableYears] = useState<string[]>([]);
  
  // Local state for company data
  const [companyData, setCompanyData] = useState<CompanyData | null>(null);
  
  // Override Workflow State
  const [overrideIndicator, setOverrideIndicator] = useState<Indicator | null>(null);
  
  // Lineage Workflow State
  const [lineageIndicator, setLineageIndicator] = useState<Indicator | null>(null);

  // Run Pipeline Modal State
  const [isRunPipelineModalOpen, setIsRunPipelineModalOpen] = useState(false);

  // Intervention Drawer State
  const [isInterventionOpen, setIsInterventionOpen] = useState(false);
  const [evidenceRefreshKey, setEvidenceRefreshKey] = useState(0);

  // Load company data from API
  useEffect(() => {
    if (!companyId) return;
    loadCompany(companyId, selectedFY || undefined);
    loadAvailableYears(companyId);
  }, [companyId]);

  const loadAvailableYears = async (id: string) => {
    try {
      const years = await api.getCompanyYears(id);
      setAvailableYears(years);
    } catch (err) {
      // Fallback: generate past 10 years
      const currentYear = new Date().getFullYear();
      setAvailableYears(Array.from({ length: 10 }, (_, i) => `FY${currentYear - i}`));
    }
  };

  const loadCompany = async (id: string, fy?: string) => {
    setIsLoading(true);
    try {
      const data = await api.getCompany(id, fy);
      setCompanyData(data);
      if (!selectedFY) setSelectedFY(data.financialYear);
    } catch (err) {
      console.error('Failed to load company:', err);
    } finally {
      setIsLoading(false);
    }
  };

  const handleFYChange = (fy: string) => {
    setSelectedFY(fy);
    if (companyId) loadCompany(companyId, fy);
  };

  const handleStartPipeline = async (config: { dataSources: string[]; financialYears: string[] }): Promise<PipelineJob[]> => {
    if (!companyId) return [];
    setIsRefreshing(true);
    try {
      const jobs = await api.runPipeline({
        company_ids: [companyId],
        data_sources: config.dataSources,
        financial_years: config.financialYears,
      });
      return jobs;
    } catch (err) {
      setIsRefreshing(false);
      throw err;
    }
  };

  const handlePipelineComplete = async () => {
    if (!companyId) {
      setIsRefreshing(false);
      return;
    }
    await loadCompany(companyId, selectedFY || undefined);
    setIsRefreshing(false);
  };

  const handleOverrideSubmit = (override: PendingOverride) => {
    if (!overrideIndicator || !companyData) return;

    // Optimistic update — the drawer has already submitted the real API call
    setCompanyData(prev => {
      if (!prev) return prev;
      return {
        ...prev,
        indicators: prev.indicators.map(ind => 
          ind.id === overrideIndicator.id 
            ? { ...ind, pendingOverride: override }
            : ind
        )
      };
    });
  };

  if (isLoading || !companyData) {
    return (
      <div className="flex-1 flex items-center justify-center bg-slate-900">
        <div className="text-center">
          <RefreshCw className="w-10 h-10 text-indigo-500 animate-spin mx-auto mb-4" />
          <p className="text-slate-400">Loading company data...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="flex-1 flex flex-col overflow-hidden relative h-full">
        {/* Background Grid Pattern */}
        <div className="absolute inset-0 bg-[url('https://grainy-gradients.vercel.app/noise.svg')] opacity-20 pointer-events-none"></div>
        
        {/* Top Header */}
        <header className="h-16 border-b border-slate-800 flex items-center justify-between px-6 bg-slate-900/80 backdrop-blur z-10">
          <div className="flex items-center gap-4">
             {/* Back Button */}
             <button onClick={onBack} className="p-1 hover:bg-slate-800 rounded text-slate-400 hover:text-white transition-colors">
                <ArrowLeft className="w-5 h-5" />
             </button>

             {/* Breadcrumb */}
             <div className="flex items-center text-sm text-slate-500">
                <span onClick={onBack} className="hover:text-slate-300 cursor-pointer transition-colors">Master Universe</span>
                <ChevronRight className="w-4 h-4 mx-2" />
                <span className="text-slate-200 font-medium flex items-center gap-2">
                  {companyData.name}
                  <span className="px-1.5 py-0.5 rounded text-[10px] font-mono bg-slate-800 text-slate-400 border border-slate-700">
                    {companyData.ticker}
                  </span>
                  <span className="px-1.5 py-0.5 rounded text-[10px] font-bold bg-indigo-500/10 text-indigo-400 border border-indigo-500/20">
                    {companyData.financialYear}
                  </span>
                </span>
             </div>
          </div>

          <div className="flex items-center gap-4">
            
            {/* Version Selector */}
            <div className="flex items-center gap-2 bg-slate-800 rounded-md px-3 py-1.5 border border-slate-700">
              <GitBranch className="w-4 h-4 text-slate-400" />
              <select className="bg-transparent text-sm text-slate-300 focus:outline-none cursor-pointer">
                 <option>{companyData.version} (Current)</option>
                 <option>v2024.04.01</option>
                 <option>v2024.01.15</option>
              </select>
            </div>

            {/* Live/Draft Toggle */}
            <div className="bg-slate-800 p-1 rounded-lg border border-slate-700 flex">
                <button 
                  onClick={() => setMode('DRAFT')}
                  className={`px-3 py-1 rounded-md text-xs font-medium transition-all ${mode === 'DRAFT' ? 'bg-amber-500 text-slate-900 shadow' : 'text-slate-400 hover:text-slate-200'}`}
                >
                  DRAFT
                </button>
                <button 
                   onClick={() => setMode('LIVE')}
                   className={`px-3 py-1 rounded-md text-xs font-medium transition-all ${mode === 'LIVE' ? 'bg-emerald-500 text-slate-900 shadow' : 'text-slate-400 hover:text-slate-200'}`}
                >
                  LIVE
                </button>
            </div>

            {userRole === 'OPERATIONS_MANAGER' && companyData.indicators.some(i => i.pendingOverride) && (
              <button className="bg-indigo-600 hover:bg-indigo-500 text-white px-4 py-1.5 rounded-lg text-xs font-bold flex items-center gap-2 shadow-lg shadow-indigo-500/20 transition-all animate-pulse">
                <Clock className="w-3.5 h-3.5" /> Submit for Approval
              </button>
            )}

            {userRole === 'ADMIN' && companyData.indicators.some(i => i.pendingOverride) && (
              <button className="bg-emerald-600 hover:bg-emerald-500 text-white px-4 py-1.5 rounded-lg text-xs font-bold flex items-center gap-2 shadow-lg shadow-emerald-500/20 transition-all">
                <CheckCircle className="w-3.5 h-3.5" /> Approve Changes
              </button>
            )}

            <button
              onClick={() => setIsInterventionOpen(true)}
              className="p-2 text-slate-400 hover:text-white transition-colors relative"
              title="Manual Intervention & Override"
            >
              <Bell className="w-5 h-5" />
              <span className="absolute top-1.5 right-1.5 w-2 h-2 bg-red-500 rounded-full"></span>
            </button>
          </div>
        </header>

        {/* Scrollable Content */}
        <div className="flex-1 overflow-y-auto p-6 scroll-smooth">
          <div className="max-w-7xl mx-auto space-y-6">
            
            {/* Risk Quadrants (2x2 Grid) */}
            <section>
              <div className="flex items-center justify-between mb-4">
                <div className="flex items-center gap-4">
                  <h2 className="text-lg font-semibold text-white">Risk Profile</h2>
                  
                  {/* FY Filter */}
                  <div className="flex items-center gap-2 bg-slate-800 p-1 rounded-lg border border-slate-700">
                    <span className="text-[10px] text-slate-500 uppercase font-bold px-2">FY</span>
                    <select 
                        value={selectedFY}
                        onChange={(e) => handleFYChange(e.target.value)}
                        className="bg-transparent text-xs text-slate-300 outline-none pr-2 cursor-pointer font-medium"
                    >
                        {(availableYears.length > 0 ? availableYears : [selectedFY]).map(fy => (
                            <option key={fy} value={fy}>{fy}</option>
                        ))}
                    </select>
                  </div>
                </div>
                <div className="flex items-center gap-2 text-xs text-slate-500">
                   <RefreshCw className="w-3 h-3" />
                   {selectedFY === 'ALL' ? 'Aggregated view' : `Showing data for ${selectedFY}`}
                </div>
              </div>
              
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4 relative">
                {isRefreshing && (
                  <div className="absolute inset-0 bg-slate-900/40 backdrop-blur-[1px] z-20 flex items-center justify-center rounded-xl">
                    <RefreshCw className="w-8 h-8 text-indigo-500 animate-spin" />
                  </div>
                )}
                <RiskCard pillar={companyData.pillars.sustainability} />
                <RiskCard pillar={companyData.pillars.pchi} />
                <RiskCard pillar={companyData.pillars.operational} />
                <RiskCard pillar={companyData.pillars.financial} />
              </div>
            </section>

            {/* Lower Section: Tabs (Data & Evidence) */}
            <section className="flex flex-col lg:flex-row gap-6 h-[600px]">
              
              {/* Left Column: Data Table */}
              <div className="flex-[2] bg-slate-900 border border-slate-800 rounded-lg overflow-hidden flex flex-col shadow-xl">
                 <div className="flex border-b border-slate-800">
                    <button 
                      onClick={() => setActiveTab('indicators')}
                      className={`px-6 py-4 text-sm font-medium transition-all border-b-2 ${activeTab === 'indicators' ? 'border-indigo-500 text-indigo-400 bg-slate-800/50' : 'border-transparent text-slate-500 hover:text-slate-300'}`}
                    >
                      Atomic Indicators
                    </button>
                    <button 
                       onClick={() => setActiveTab('evidence')}
                       className={`lg:hidden px-6 py-4 text-sm font-medium transition-all border-b-2 ${activeTab === 'evidence' ? 'border-indigo-500 text-indigo-400 bg-slate-800/50' : 'border-transparent text-slate-500 hover:text-slate-300'}`}
                    >
                      Evidence Locker
                    </button>
                 </div>
                 
                 <div className="flex-1 overflow-y-auto bg-slate-900/50">
                    {/* Search / Filter Bar for Table */}
                    <div className="p-4 border-b border-slate-800 flex items-center gap-2">
                       <Search className="w-4 h-4 text-slate-500" />
                       <input 
                         type="text" 
                         placeholder="Search data points (e.g. 'Emissions')..." 
                         className="bg-transparent border-none focus:outline-none text-sm text-slate-200 w-full"
                       />
                    </div>
                    
                    <IndicatorTable 
                        company={companyData} 
                        onOverrideClick={setOverrideIndicator}
                        onLineageClick={setLineageIndicator}
                    />
                 </div>
              </div>

              {/* Right Column: Evidence Locker (Always visible on large screens) */}
              <div className={`
                 flex-1 bg-slate-900 border border-slate-800 rounded-lg p-4 shadow-xl flex flex-col
                 ${activeTab === 'indicators' ? 'hidden lg:flex' : 'flex'}
              `}>
                <EvidencePanel company={companyData} refreshKey={evidenceRefreshKey} />
              </div>

            </section>

          </div>
        </div>

        {/* Persistent Floating Action Button for Re-run */}
        <div className="absolute bottom-8 right-8 z-30">
           <button 
             onClick={() => setIsRunPipelineModalOpen(true)}
             className="flex items-center gap-2 bg-indigo-600 hover:bg-indigo-500 text-white px-5 py-3 rounded-full shadow-lg shadow-indigo-900/50 font-medium transition-transform hover:scale-105 active:scale-95"
           >
             <Play className="w-4 h-4 fill-current" />
             Run Pipeline
           </button>
        </div>

        <RunPipelineModal 
          isOpen={isRunPipelineModalOpen}
          onClose={() => setIsRunPipelineModalOpen(false)}
          onStart={handleStartPipeline}
          onJobsComplete={handlePipelineComplete}
          initialFinancialYear={selectedFY || companyData.financialYear}
        />

        <OverrideProposalDrawer
            isOpen={!!overrideIndicator}
            onClose={() => setOverrideIndicator(null)}
            indicator={overrideIndicator}
            onSubmit={handleOverrideSubmit}
            userRole={userRole}
            companyId={companyData.id}
        />

        <LineageDrawer
            isOpen={!!lineageIndicator}
            onClose={() => setLineageIndicator(null)}
            indicator={lineageIndicator}
            companyId={companyData.id}
        />

        <InterventionDrawer
            isOpen={isInterventionOpen}
            onClose={() => setIsInterventionOpen(false)}
            company={companyData}
            onEvidenceAdded={() => setEvidenceRefreshKey(k => k + 1)}
        />
    </div>
  );
}

export default CompanyDetail;
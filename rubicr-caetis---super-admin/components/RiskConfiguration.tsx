import React, { useState, useEffect } from 'react';
import { 
  Save, 
  AlertTriangle, 
  Shield, 
  Globe, 
  Trash2, 
  Plus, 
  CheckCircle, 
  RefreshCw,
  Sliders,
  Info,
  Play,
  LayoutGrid,
  Activity,
  Edit2,
  ExternalLink,
  MessageSquare,
  Share2,
  Newspaper,
  Globe2,
  Database,
  Ban,
  XCircle
} from 'lucide-react';
import RiskSimulationModal from './RiskSimulationModal';
import GlobalSourceModal from './GlobalSourceModal';
import api from '../apiService';

interface DriverWeight {
  id: string;
  name: string;
  category: 'Sustainability' | 'PCHI' | 'Operational' | 'Financial';
  weight: number;
}

interface DomainRule {
  id: string;
  domain: string;
  type: 'SECONDARY' | 'TERTIARY';
  subType?: string;
  status: 'ACTIVE' | 'INACTIVE';
  addedBy: string;
  date: string;
}

const RiskConfiguration: React.FC = () => {
  const [activeTab, setActiveTab] = useState<'WEIGHTS' | 'THRESHOLDS' | 'SOURCES'>('WEIGHTS');
  const [isSimulationOpen, setIsSimulationOpen] = useState(false);
  const [isAddSourceOpen, setIsAddSourceOpen] = useState(false);
  const [mode, setMode] = useState<'PRODUCTION' | 'DRAFT'>('PRODUCTION');
  const [isSaving, setIsSaving] = useState(false);
  const [companyCount, setCompanyCount] = useState(0);

  // --- State: Blocked Sources ---
  const [blockedUrls, setBlockedUrls] = useState<{id: string, url: string, date: string}[]>([]);
  const [blockInput, setBlockInput] = useState('');

  const handleBlockUrls = async () => {
    if (!blockInput) return;
    const urls = blockInput.split(/[\n,]+/).map(u => u.trim()).filter(u => u.length > 0);
    try {
      const newBlocked = await api.blockUrls(urls);
      setBlockedUrls(prev => [...newBlocked, ...prev]);
      setBlockInput('');
    } catch (err) {
      console.error('Block URLs failed:', err);
    }
  };

  const unblockUrl = async (id: string) => {
    try {
      await api.unblockUrl(id);
      setBlockedUrls(prev => prev.filter(b => b.id !== id));
    } catch (err) {
      console.error('Unblock failed:', err);
    }
  };

  // --- State: Weights ---
  const [weights, setWeights] = useState<DriverWeight[]>([]);

  const totalWeight = weights.reduce((sum, w) => sum + w.weight, 0);

  const handleWeightChange = (id: string, newValue: number) => {
    setWeights(weights.map(w => w.id === id ? { ...w, weight: newValue } : w));
    setMode('DRAFT');
  };

  // --- State: Thresholds ---
  const [thresholds, setThresholds] = useState({
    medium: 45,
    high: 75
  });

  // --- State: Sources ---
  const [domains, setDomains] = useState<DomainRule[]>([]);

  // Load all config on mount
  useEffect(() => {
    api.getWeights().then(w => setWeights(w as DriverWeight[])).catch(console.error);
    api.getThresholds().then(t => setThresholds(t)).catch(console.error);
    api.getDomains().then(d => setDomains(d.map(r => ({
      id: r.id,
      domain: r.domain,
      type: r.type as 'SECONDARY' | 'TERTIARY',
      subType: r.sub_type,
      status: r.status as 'ACTIVE' | 'INACTIVE',
      addedBy: r.added_by,
      date: r.date
    })))).catch(console.error);
    api.getBlockedUrls().then(b => setBlockedUrls(b.map(u => ({
      id: u.id, url: u.url, date: u.date
    })))).catch(console.error);
    api.getCompanies().then(c => setCompanyCount(c.length)).catch(console.error);
  }, []);

  const handleAddSource = async (source: { domain: string; type: 'SECONDARY' | 'TERTIARY'; subType?: string }) => {
    try {
      const newDomain = await api.addDomain({
        domain: source.domain,
        type: source.type,
        sub_type: source.subType,
      });
      setDomains(prev => [...prev, {
        id: newDomain.id,
        domain: newDomain.domain,
        type: newDomain.type as 'SECONDARY' | 'TERTIARY',
        subType: newDomain.sub_type,
        status: newDomain.status as 'ACTIVE' | 'INACTIVE',
        addedBy: newDomain.added_by,
        date: newDomain.date
      }]);
    } catch (err) {
      console.error('Add domain failed:', err);
    }
  };

  const removeDomain = async (id: string) => {
    try {
      await api.deleteDomain(id);
      setDomains(prev => prev.filter(d => d.id !== id));
    } catch (err) {
      console.error('Remove domain failed:', err);
    }
  };

  const toggleDomain = async (id: string) => {
    try {
      const updated = await api.toggleDomain(id);
      setDomains(prev => prev.map(d => d.id === id ? {
        ...d,
        status: updated.status as 'ACTIVE' | 'INACTIVE'
      } : d));
    } catch (err) {
      console.error('Toggle domain failed:', err);
    }
  };

  const handleSaveWeights = async () => {
    setIsSaving(true);
    try {
      await api.updateWeights(weights);
      setMode('PRODUCTION');
    } catch (err) {
      console.error('Save weights failed:', err);
    } finally {
      setIsSaving(false);
    }
  };

  const handleSaveThresholds = async () => {
    setIsSaving(true);
    try {
      await api.updateThresholds(thresholds);
      setMode('PRODUCTION');
    } catch (err) {
      console.error('Save thresholds failed:', err);
    } finally {
      setIsSaving(false);
    }
  };

  const handleSimulationSubmit = () => {
    setIsSimulationOpen(false);
    alert("Configuration submitted for Lead Approval!");
    setMode('PRODUCTION');
  };

  return (
    <div className="flex-1 flex flex-col h-full bg-slate-950 overflow-hidden relative">
      {/* Background Grid Pattern */}
      <div className="absolute inset-0 bg-[url('https://grainy-gradients.vercel.app/noise.svg')] opacity-20 pointer-events-none"></div>

      {/* Header */}
      <header className="h-20 border-b border-slate-800 bg-slate-900/80 backdrop-blur z-10 px-8 flex items-center justify-between shrink-0">
        <div>
            <h1 className="text-xl font-bold text-white tracking-tight flex items-center gap-3">
                <Sliders className="w-6 h-6 text-indigo-500" />
                Global Risk Engine Configuration
            </h1>
            <div className="flex items-center gap-2 mt-1">
                <span className={`px-2 py-0.5 rounded text-[10px] font-bold uppercase tracking-wider border ${
                    mode === 'PRODUCTION' 
                    ? 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20' 
                    : 'bg-amber-500/10 text-amber-400 border-amber-500/20'
                }`}>
                    {mode === 'PRODUCTION' ? 'Production Mode' : 'Draft Mode (Unsaved)'}
                </span>
                <span className="text-xs text-slate-500">v2.4.0</span>
            </div>
        </div>
        
        <div className="flex items-center gap-3">
            <button 
                onClick={() => setIsSimulationOpen(true)}
                disabled={totalWeight !== 100}
                className={`
                    px-5 py-2.5 rounded-lg text-sm font-medium flex items-center gap-2 shadow-lg transition-all
                    ${totalWeight === 100 
                        ? 'bg-indigo-600 hover:bg-indigo-500 text-white shadow-indigo-500/20 hover:scale-105 active:scale-95' 
                        : 'bg-slate-800 text-slate-500 cursor-not-allowed'}
                `}
            >
                <Play className="w-4 h-4 fill-current" /> Run Impact Simulation
            </button>
        </div>
      </header>

      {/* Tabs */}
      <div className="px-8 pt-6 border-b border-slate-800 bg-slate-900/50 backdrop-blur z-10">
        <div className="flex items-center gap-8">
            <button 
                onClick={() => setActiveTab('WEIGHTS')}
                className={`pb-4 text-sm font-medium transition-all relative flex items-center gap-2 ${activeTab === 'WEIGHTS' ? 'text-indigo-400' : 'text-slate-500 hover:text-slate-300'}`}
            >
                <LayoutGrid className="w-4 h-4" /> Pillar Weightings
                {activeTab === 'WEIGHTS' && <div className="absolute bottom-0 left-0 right-0 h-0.5 bg-indigo-500"></div>}
            </button>
            <button 
                onClick={() => setActiveTab('THRESHOLDS')}
                className={`pb-4 text-sm font-medium transition-all relative flex items-center gap-2 ${activeTab === 'THRESHOLDS' ? 'text-indigo-400' : 'text-slate-500 hover:text-slate-300'}`}
            >
                <Activity className="w-4 h-4" /> Risk Thresholds
                {activeTab === 'THRESHOLDS' && <div className="absolute bottom-0 left-0 right-0 h-0.5 bg-indigo-500"></div>}
            </button>
            <button 
                onClick={() => setActiveTab('SOURCES')}
                className={`pb-4 text-sm font-medium transition-all relative flex items-center gap-2 ${activeTab === 'SOURCES' ? 'text-indigo-400' : 'text-slate-500 hover:text-slate-300'}`}
            >
                <Shield className="w-4 h-4" /> Global Source Control
                {activeTab === 'SOURCES' && <div className="absolute bottom-0 left-0 right-0 h-0.5 bg-indigo-500"></div>}
            </button>
        </div>
      </div>

      {/* Main Content Area */}
      <main className="flex-1 overflow-y-auto p-8 max-w-6xl mx-auto w-full z-0">
        
        {/* Tab 1: Weights */}
        {activeTab === 'WEIGHTS' && (
            <div className="space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
                <div className="bg-slate-900 border border-slate-800 rounded-xl p-6 shadow-xl relative overflow-hidden">
                    <div className="flex justify-between items-start mb-8">
                        <div>
                            <h2 className="text-lg font-semibold text-white">Algorithm Weights</h2>
                            <p className="text-sm text-slate-500">Adjust the influence of each risk driver on the global company score.</p>
                        </div>
                        <div className={`px-4 py-2 rounded-lg font-mono text-sm font-bold border flex items-center gap-2 ${totalWeight === 100 ? 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20' : 'bg-red-500/10 text-red-400 border-red-500/20'}`}>
                            {totalWeight !== 100 && <AlertTriangle className="w-4 h-4" />}
                            Total: {totalWeight}%
                        </div>
                    </div>

                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-x-16 gap-y-8">
                        {weights.map((driver) => (
                            <div key={driver.id} className="group">
                                <div className="flex justify-between text-sm mb-2">
                                    <span className="text-slate-200 font-medium group-hover:text-indigo-300 transition-colors">{driver.name}</span>
                                    <span className="text-slate-400 font-mono">{driver.weight}%</span>
                                </div>
                                <div className="flex items-center gap-4">
                                    <input 
                                        type="range" 
                                        min="0" 
                                        max="100" 
                                        step="5"
                                        value={driver.weight}
                                        onChange={(e) => handleWeightChange(driver.id, parseInt(e.target.value))}
                                        className="flex-1 h-2 bg-slate-800 rounded-lg appearance-none cursor-pointer accent-indigo-500 hover:accent-indigo-400 transition-all"
                                    />
                                    <div className="w-24 text-right">
                                        <span className={`text-[10px] px-2 py-1 rounded-full border uppercase tracking-wider font-bold ${
                                            driver.category === 'Sustainability' ? 'border-emerald-500/30 text-emerald-400 bg-emerald-500/5' :
                                            driver.category === 'PCHI' ? 'border-blue-500/30 text-blue-400 bg-blue-500/5' :
                                            driver.category === 'Operational' ? 'border-amber-500/30 text-amber-400 bg-amber-500/5' :
                                            'border-purple-500/30 text-purple-400 bg-purple-500/5'
                                        }`}>
                                            {driver.category.substring(0, 3)}
                                        </span>
                                    </div>
                                </div>
                            </div>
                        ))}
                    </div>
                    <div className="mt-8 flex justify-end">
                        <button
                            onClick={handleSaveWeights}
                            disabled={totalWeight !== 100 || isSaving}
                            className={`px-6 py-2.5 rounded-lg text-sm font-medium flex items-center gap-2 transition-all shadow-lg ${
                                totalWeight === 100 && !isSaving
                                    ? 'bg-indigo-600 hover:bg-indigo-500 text-white shadow-indigo-500/20 hover:scale-105 active:scale-95'
                                    : 'bg-slate-800 text-slate-500 cursor-not-allowed'
                            }`}
                        >
                            <Save className="w-4 h-4" />
                            {isSaving ? 'Saving...' : 'Save Weights'}
                        </button>
                    </div>
                </div>
            </div>
        )}
        {activeTab === 'THRESHOLDS' && (
            <div className="space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
                <div className="bg-slate-900 border border-slate-800 rounded-xl p-8 shadow-xl">
                    <div className="mb-8">
                        <h2 className="text-lg font-semibold text-white">Risk Thresholds</h2>
                        <p className="text-sm text-slate-500">Define the score boundaries that trigger risk alerts across the platform.</p>
                    </div>

                    <div className="relative py-12 px-8 bg-slate-950 rounded-xl border border-slate-800">
                        {/* Gradient Bar */}
                        <div className="h-6 w-full rounded-full bg-gradient-to-r from-emerald-500 via-amber-500 to-red-500 relative shadow-inner">
                            {/* Markers */}
                            <div 
                                className="absolute top-1/2 -translate-y-1/2 w-1.5 h-10 bg-white border-2 border-slate-900 cursor-ew-resize shadow-xl z-10 rounded-full hover:scale-110 transition-transform"
                                style={{ left: `${thresholds.medium}%` }}
                                title="Medium Risk Threshold"
                            >
                                <div className="absolute -top-8 left-1/2 -translate-x-1/2 bg-slate-800 text-white text-xs px-2 py-1 rounded font-mono border border-slate-700">
                                    {thresholds.medium}
                                </div>
                            </div>
                             <div 
                                className="absolute top-1/2 -translate-y-1/2 w-1.5 h-10 bg-white border-2 border-slate-900 cursor-ew-resize shadow-xl z-10 rounded-full hover:scale-110 transition-transform"
                                style={{ left: `${thresholds.high}%` }}
                                title="High Risk Threshold"
                            >
                                <div className="absolute -top-8 left-1/2 -translate-x-1/2 bg-slate-800 text-white text-xs px-2 py-1 rounded font-mono border border-slate-700">
                                    {thresholds.high}
                                </div>
                            </div>
                        </div>

                        <div className="flex justify-between mt-8 text-center divide-x divide-slate-800">
                            <div className="flex-1 px-4">
                                <div className="text-emerald-500 font-bold text-2xl mb-1">&lt; {thresholds.medium}</div>
                                <div className="text-xs text-slate-500 uppercase tracking-wide font-bold">Low Risk</div>
                                <p className="text-xs text-slate-600 mt-2">Companies in this range are considered safe and compliant.</p>
                            </div>
                            <div className="flex-1 px-4">
                                <div className="text-amber-500 font-bold text-2xl mb-1">{thresholds.medium} - {thresholds.high}</div>
                                <div className="text-xs text-slate-500 uppercase tracking-wide font-bold">Medium Risk</div>
                                <p className="text-xs text-slate-600 mt-2">Companies require monitoring and potential mitigation.</p>
                            </div>
                            <div className="flex-1 px-4">
                                <div className="text-red-500 font-bold text-2xl mb-1">&gt; {thresholds.high}</div>
                                <div className="text-xs text-slate-500 uppercase tracking-wide font-bold">High Risk</div>
                                <p className="text-xs text-slate-600 mt-2">Immediate intervention required. Critical alerts triggered.</p>
                            </div>
                        </div>
                    </div>

                    <div className="mt-8 grid grid-cols-2 gap-6 max-w-lg mx-auto">
                        <div>
                             <label className="block text-xs text-slate-500 mb-2 uppercase tracking-wider font-bold">Amber Threshold Start</label>
                             <input 
                                type="number" 
                                value={thresholds.medium}
                                onChange={(e) => {
                                    setThresholds({...thresholds, medium: parseInt(e.target.value)});
                                    setMode('DRAFT');
                                }}
                                className="w-full bg-slate-950 border border-slate-700 rounded-lg px-4 py-3 text-white focus:outline-none focus:border-indigo-500 font-mono text-lg"
                             />
                        </div>
                        <div>
                             <label className="block text-xs text-slate-500 mb-2 uppercase tracking-wider font-bold">Red Threshold Start</label>
                             <input 
                                type="number" 
                                value={thresholds.high}
                                onChange={(e) => {
                                    setThresholds({...thresholds, high: parseInt(e.target.value)});
                                    setMode('DRAFT');
                                }}
                                className="w-full bg-slate-950 border border-slate-700 rounded-lg px-4 py-3 text-white focus:outline-none focus:border-indigo-500 font-mono text-lg"
                             />
                        </div>
                    </div>
                    <div className="mt-8 flex justify-end">
                        <button
                            onClick={handleSaveThresholds}
                            disabled={isSaving}
                            className="px-6 py-2.5 rounded-lg text-sm font-medium flex items-center gap-2 transition-all shadow-lg bg-indigo-600 hover:bg-indigo-500 text-white shadow-indigo-500/20 hover:scale-105 active:scale-95 disabled:opacity-50 disabled:cursor-not-allowed"
                        >
                            <Save className="w-4 h-4" />
                            {isSaving ? 'Saving...' : 'Save Thresholds'}
                        </button>
                    </div>
                </div>
            </div>
        )}

        {/* Tab 3: Sources */}
        {activeTab === 'SOURCES' && (
            <div className="space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
                <div className="bg-slate-900 border border-slate-800 rounded-xl p-6 shadow-xl">
                     <div className="mb-8 flex justify-between items-center">
                        <div>
                            <h2 className="text-lg font-semibold text-white flex items-center gap-2">
                                <Shield className="w-5 h-5 text-indigo-400" />
                                Global Source Intelligence Control
                            </h2>
                            <p className="text-sm text-slate-500">Manage the global pool of domains used for risk data extraction.</p>
                        </div>
                        <button 
                            onClick={() => setIsAddSourceOpen(true)}
                            className="bg-indigo-600 hover:bg-indigo-500 text-white px-4 py-2 rounded-lg text-sm font-medium flex items-center gap-2 transition-all shadow-lg shadow-indigo-500/20"
                        >
                            <Plus className="w-4 h-4" /> Add Source
                        </button>
                    </div>

                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
                        {/* Secondary Sources Column */}
                        <div className="space-y-4">
                            <div className="flex items-center gap-2 px-2">
                                <Database className="w-4 h-4 text-emerald-400" />
                                <h3 className="text-sm font-bold text-slate-300 uppercase tracking-wider">Secondary Data Sources</h3>
                                <span className="ml-auto text-[10px] font-bold text-slate-500 bg-slate-800 px-2 py-0.5 rounded">
                                    {domains.filter(d => d.type === 'SECONDARY').length} Domains
                                </span>
                            </div>
                            <div className="border border-slate-800 rounded-xl overflow-hidden bg-slate-950/50">
                                <SourceTable 
                                    sources={domains.filter(d => d.type === 'SECONDARY')} 
                                    onDelete={removeDomain}
                                    onToggle={toggleDomain}
                                />
                            </div>
                        </div>

                        {/* Tertiary Sources Column */}
                        <div className="space-y-4">
                            <div className="flex items-center gap-2 px-2">
                                <Globe2 className="w-4 h-4 text-amber-400" />
                                <h3 className="text-sm font-bold text-slate-300 uppercase tracking-wider">Tertiary Data Sources</h3>
                                <span className="ml-auto text-[10px] font-bold text-slate-500 bg-slate-800 px-2 py-0.5 rounded">
                                    {domains.filter(d => d.type === 'TERTIARY').length} Domains
                                </span>
                            </div>
                            <div className="border border-slate-800 rounded-xl overflow-hidden bg-slate-950/50">
                                <SourceTable 
                                    sources={domains.filter(d => d.type === 'TERTIARY')} 
                                    onDelete={removeDomain}
                                    onToggle={toggleDomain}
                                />
                            </div>
                        </div>
                    </div>

                    {/* Blocked Sources Section */}
                    <div className="mt-12 pt-12 border-t border-slate-800">
                        <div className="flex items-center gap-2 mb-6">
                            <Ban className="w-5 h-5 text-red-500" />
                            <div>
                                <h3 className="text-lg font-bold text-white">Blocked Intelligence Sources</h3>
                                <p className="text-sm text-slate-500">Domains and URLs explicitly ignored by the global risk engine.</p>
                            </div>
                        </div>

                        <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
                            {/* Block Input */}
                            <div className="lg:col-span-1 space-y-4">
                                <div className="bg-slate-950 p-6 rounded-2xl border border-slate-800 space-y-4">
                                    <div className="space-y-2">
                                        <label className="block text-xs font-bold text-slate-500 uppercase tracking-wider">Add Blocked URLs</label>
                                        <textarea 
                                            value={blockInput}
                                            onChange={(e) => setBlockInput(e.target.value)}
                                            placeholder="Enter URLs (one per line or comma separated)..."
                                            className="w-full h-32 bg-slate-900 border border-slate-800 rounded-xl p-4 text-sm text-white placeholder:text-slate-700 focus:outline-none focus:border-red-500/50 transition-all resize-none"
                                        />
                                        <p className="text-[10px] text-slate-600 italic">Example: spam.com, malicious-site.org</p>
                                    </div>
                                    <button 
                                        onClick={handleBlockUrls}
                                        disabled={!blockInput}
                                        className="w-full py-3 bg-red-500/10 hover:bg-red-500/20 text-red-400 border border-red-500/20 rounded-xl text-sm font-bold flex items-center justify-center gap-2 transition-all disabled:opacity-50"
                                    >
                                        <XCircle className="w-4 h-4" /> Block Sources
                                    </button>
                                </div>
                            </div>

                            {/* Blocked List */}
                            <div className="lg:col-span-2">
                                <div className="border border-slate-800 rounded-xl overflow-hidden bg-slate-950/50">
                                    <table className="w-full text-left text-xs">
                                        <thead className="bg-slate-900/80 text-slate-500 font-bold border-b border-slate-800 uppercase tracking-widest">
                                            <tr>
                                                <th className="px-6 py-4">Blocked Domain / URL</th>
                                                <th className="px-6 py-4">Date Added</th>
                                                <th className="px-6 py-4 text-right">Action</th>
                                            </tr>
                                        </thead>
                                        <tbody className="divide-y divide-slate-800/50">
                                            {blockedUrls.map((item) => (
                                                <tr key={item.id} className="group hover:bg-red-500/5 transition-colors">
                                                    <td className="px-6 py-4 font-mono text-slate-300">
                                                        <div className="flex items-center gap-3">
                                                            <div className="w-2 h-2 rounded-full bg-red-500/50 shadow-[0_0_8px_rgba(239,68,68,0.5)]" />
                                                            {item.url}
                                                        </div>
                                                    </td>
                                                    <td className="px-6 py-4 text-slate-500">{item.date}</td>
                                                    <td className="px-6 py-4 text-right">
                                                        <button 
                                                            onClick={() => unblockUrl(item.id)}
                                                            className="p-2 hover:bg-emerald-500/10 rounded text-slate-600 hover:text-emerald-400 transition-colors"
                                                            title="Unblock Source"
                                                        >
                                                            <RefreshCw className="w-4 h-4" />
                                                        </button>
                                                    </td>
                                                </tr>
                                            ))}
                                            {blockedUrls.length === 0 && (
                                                <tr>
                                                    <td colSpan={3} className="px-6 py-12 text-center text-slate-600 italic">
                                                        No blocked sources found.
                                                    </td>
                                                </tr>
                                            )}
                                        </tbody>
                                    </table>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        )}

      </main>

      {/* Sticky Footer for Validation Error */}
      {totalWeight !== 100 && activeTab === 'WEIGHTS' && (
         <div className="absolute bottom-6 left-1/2 -translate-x-1/2 bg-red-500 text-white px-6 py-3 rounded-full shadow-2xl flex items-center gap-3 animate-bounce z-20">
            <AlertTriangle className="w-5 h-5 fill-white text-red-600" />
            <span className="font-medium">Total weight must equal 100% (Current: {totalWeight}%)</span>
         </div>
      )}

      <RiskSimulationModal 
        isOpen={isSimulationOpen}
        onClose={() => setIsSimulationOpen(false)}
        onSubmit={handleSimulationSubmit}
        companyCount={companyCount}
      />

      <GlobalSourceModal 
        isOpen={isAddSourceOpen}
        onClose={() => setIsAddSourceOpen(false)}
        onAdd={handleAddSource}
      />
    </div>
  );
};

// Sub-component for Source Table
const SourceTable = ({ sources, onDelete, onToggle }: { sources: DomainRule[], onDelete: (id: string) => void, onToggle: (id: string) => void }) => {
    return (
        <table className="w-full text-left text-xs">
            <thead className="bg-slate-900/80 text-slate-500 font-bold border-b border-slate-800 uppercase tracking-widest">
                <tr>
                    <th className="px-4 py-3">Domain</th>
                    <th className="px-4 py-3">Status</th>
                    <th className="px-4 py-3">Added By</th>
                    <th className="px-4 py-3 text-right">Actions</th>
                </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/50">
                {sources.map((rule) => (
                    <tr key={rule.id} className="group hover:bg-slate-800/30 transition-colors">
                        <td className="px-4 py-3">
                            <div className="flex flex-col">
                                <span className="font-bold text-slate-200 flex items-center gap-2">
                                    <Globe className="w-3 h-3 text-slate-500" />
                                    {rule.domain}
                                </span>
                                {rule.subType && (
                                    <span className="text-[9px] text-indigo-400 font-bold uppercase mt-0.5 flex items-center gap-1">
                                        {rule.subType === 'Social Media' && <Share2 className="w-2.5 h-2.5" />}
                                        {rule.subType === 'News' && <Newspaper className="w-2.5 h-2.5" />}
                                        {rule.subType === 'Forum' && <Globe2 className="w-2.5 h-2.5" />}
                                        {rule.subType === 'Blog' && <MessageSquare className="w-2.5 h-2.5" />}
                                        {rule.subType}
                                    </span>
                                )}
                            </div>
                        </td>
                        <td className="px-4 py-3">
                            <button
                                onClick={() => onToggle(rule.id)}
                                className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[9px] font-bold border cursor-pointer hover:opacity-80 transition-opacity ${
                                rule.status === 'ACTIVE' 
                                ? 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20' 
                                : 'bg-slate-800 text-slate-500 border-slate-700'
                            }`}>
                                {rule.status}
                            </button>
                        </td>
                        <td className="px-4 py-3">
                            <div className="flex flex-col">
                                <span className="text-slate-400 font-medium">{rule.addedBy}</span>
                                <span className="text-[9px] text-slate-600">{rule.date}</span>
                            </div>
                        </td>
                        <td className="px-4 py-3 text-right">
                            <div className="flex justify-end gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                                <button className="p-1.5 hover:bg-slate-800 rounded text-slate-500 hover:text-indigo-400 transition-colors">
                                    <Edit2 className="w-3 h-3" />
                                </button>
                                <button 
                                    onClick={() => onDelete(rule.id)}
                                    className="p-1.5 hover:bg-red-500/10 rounded text-slate-500 hover:text-red-400 transition-colors"
                                >
                                    <Trash2 className="w-3 h-3" />
                                </button>
                            </div>
                        </td>
                    </tr>
                ))}
                {sources.length === 0 && (
                    <tr>
                        <td colSpan={4} className="px-4 py-8 text-center text-slate-600 italic">
                            No sources configured in this category.
                        </td>
                    </tr>
                )}
            </tbody>
        </table>
    );
};

export default RiskConfiguration;
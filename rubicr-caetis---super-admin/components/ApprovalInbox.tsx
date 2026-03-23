import React, { useState, useEffect } from 'react';
import { 
  CheckCircle2, 
  XCircle, 
  Clock, 
  FileText, 
  Link as LinkIcon, 
  ArrowRight, 
  Search, 
  Filter,
  ShieldCheck,
  AlertTriangle,
  RefreshCw
} from 'lucide-react';
import { ApprovalRequest } from '../types';
import api from '../apiService';

const ApprovalInbox: React.FC = () => {
  const [activeTab, setActiveTab] = useState<'OVERRIDES' | 'SOURCES'>('OVERRIDES');
  const [requests, setRequests] = useState<ApprovalRequest[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [rejectModalOpen, setRejectModalOpen] = useState<string | null>(null);
  const [rejectionReason, setRejectionReason] = useState('');
  const [searchQuery, setSearchQuery] = useState('');

  useEffect(() => {
    loadApprovals();
  }, []);

  const loadApprovals = async () => {
    setIsLoading(true);
    try {
      const data = await api.getApprovals('PENDING');
      setRequests(data);
    } catch (err) {
      console.error('Failed to load approvals:', err);
    } finally {
      setIsLoading(false);
    }
  };

  const filteredRequests = requests.filter(r => {
    const matchesTab = activeTab === 'OVERRIDES' ? r.type === 'OVERRIDE' : r.type === 'SOURCE';
    const matchesStatus = r.status === 'PENDING';
    const q = searchQuery.toLowerCase().trim();
    const matchesSearch = !q ||
      (r.companyName?.toLowerCase().includes(q) ?? false) ||
      (r.indicatorName?.toLowerCase().includes(q) ?? false) ||
      r.submittedBy.toLowerCase().includes(q) ||
      (r.sourceName?.toLowerCase().includes(q) ?? false);
    return matchesTab && matchesStatus && matchesSearch;
  });

  const handleApprove = async (id: string) => {
    try {
      const updated = await api.approveRequest(id, 'Admin');
      setRequests(prev => prev.map(req => req.id === id ? updated : req));
    } catch (err) {
      console.error('Approve failed:', err);
    }
  };

  const handleRejectSubmit = async () => {
    if (!rejectModalOpen) return;
    try {
      const updated = await api.rejectRequest(rejectModalOpen, rejectionReason);
      setRequests(prev => prev.map(req => req.id === rejectModalOpen ? updated : req));
    } catch (err) {
      console.error('Reject failed:', err);
    }
    setRejectModalOpen(null);
    setRejectionReason('');
  };

  return (
    <div className="flex-1 flex flex-col h-full bg-slate-950 text-slate-200 font-sans relative overflow-hidden">
       {/* Background Grid Pattern */}
       <div className="absolute inset-0 bg-[url('https://grainy-gradients.vercel.app/noise.svg')] opacity-20 pointer-events-none"></div>

      {/* Header */}
      <header className="h-16 border-b border-slate-800 flex items-center justify-between px-8 bg-slate-900/80 backdrop-blur z-10">
        <div className="flex items-center gap-3">
          <div className="p-2 bg-indigo-500/20 rounded-lg">
            <ShieldCheck className="w-6 h-6 text-indigo-400" />
          </div>
          <div>
            <h1 className="text-lg font-semibold text-white">Approval Inbox</h1>
            <p className="text-xs text-slate-500">Manager Command Center</p>
          </div>
        </div>
        
        <div className="flex items-center gap-4">
            <button onClick={loadApprovals} className="p-2 text-slate-400 hover:text-white transition-colors" title="Refresh">
                <RefreshCw className={`w-5 h-5 ${isLoading ? 'animate-spin' : ''}`} />
            </button>
            <div className="relative">
                <Search className="absolute left-3 top-2.5 w-4 h-4 text-slate-500" />
                <input 
                    type="text" 
                    placeholder="Search requests..." 
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    className="bg-slate-900 border border-slate-700 rounded-full pl-10 pr-4 py-2 text-sm text-slate-200 focus:outline-none focus:border-indigo-500 w-64"
                />
            </div>
            <button className="p-2 text-slate-400 hover:text-white transition-colors relative">
                <Filter className="w-5 h-5" />
            </button>
        </div>
      </header>

      {/* Main Content */}
      <main className="flex-1 overflow-hidden flex flex-col p-8 max-w-6xl mx-auto w-full z-10">
        
        {/* Tabs */}
        <div className="flex items-center gap-6 border-b border-slate-800 mb-8">
            <button 
                onClick={() => setActiveTab('OVERRIDES')}
                className={`pb-4 text-sm font-medium transition-all relative ${activeTab === 'OVERRIDES' ? 'text-indigo-400' : 'text-slate-500 hover:text-slate-300'}`}
            >
                Pending Overrides
                <span className="ml-2 bg-slate-800 text-slate-400 px-2 py-0.5 rounded-full text-xs">
                    {requests.filter(r => r.type === 'OVERRIDE' && r.status === 'PENDING').length}
                </span>
                {activeTab === 'OVERRIDES' && <div className="absolute bottom-0 left-0 right-0 h-0.5 bg-indigo-500"></div>}
            </button>
            <button 
                onClick={() => setActiveTab('SOURCES')}
                className={`pb-4 text-sm font-medium transition-all relative ${activeTab === 'SOURCES' ? 'text-indigo-400' : 'text-slate-500 hover:text-slate-300'}`}
            >
                Pending Sources
                <span className="ml-2 bg-slate-800 text-slate-400 px-2 py-0.5 rounded-full text-xs">
                    {requests.filter(r => r.type === 'SOURCE' && r.status === 'PENDING').length}
                </span>
                {activeTab === 'SOURCES' && <div className="absolute bottom-0 left-0 right-0 h-0.5 bg-indigo-500"></div>}
            </button>
        </div>

        {/* Request List */}
        <div className="flex-1 overflow-y-auto space-y-4 pr-2">
            {isLoading ? (
                <div className="flex flex-col items-center justify-center h-64 text-slate-500">
                    <RefreshCw className="w-8 h-8 mb-4 opacity-40 animate-spin" />
                    <p>Loading approvals...</p>
                </div>
            ) : filteredRequests.length === 0 ? (
                <div className="flex flex-col items-center justify-center h-64 text-slate-500">
                    <CheckCircle2 className="w-12 h-12 mb-4 opacity-20" />
                    <p>All caught up! No pending requests.</p>
                </div>
            ) : (
                filteredRequests.map(request => (
                    <div key={request.id} className="bg-slate-900 border border-slate-800 rounded-xl p-6 shadow-lg hover:border-slate-700 transition-all group">
                        <div className="flex items-start justify-between">
                            
                            {/* Left: Info */}
                            <div className="flex-1">
                                <div className="flex items-center gap-3 mb-2">
                                    <span className="text-sm font-bold text-slate-200">{request.companyName}</span>
                                    <span className="text-xs font-mono bg-slate-800 text-slate-400 px-1.5 py-0.5 rounded border border-slate-700">{request.companyTicker}</span>
                                    <span className="text-xs text-slate-500 flex items-center gap-1">
                                        <Clock className="w-3 h-3" /> {new Date(request.submittedAt).toLocaleDateString()}
                                    </span>
                                </div>

                                <div className="mb-4">
                                    <p className="text-xs text-slate-500 uppercase tracking-wider font-semibold mb-1">
                                        {request.type === 'OVERRIDE' ? 'Indicator Override' : 'New Data Source'}
                                    </p>
                                    
                                    {request.type === 'OVERRIDE' ? (
                                        <div className="flex items-center gap-4 mt-2">
                                            <div className="bg-slate-950 px-3 py-2 rounded border border-slate-800">
                                                <span className="block text-[10px] text-slate-500 uppercase">Indicator</span>
                                                <span className="text-sm font-medium text-slate-300">{request.indicatorName}</span>
                                            </div>
                                            <ArrowRight className="w-4 h-4 text-slate-600" />
                                            <div className="bg-red-900/10 px-3 py-2 rounded border border-red-900/30">
                                                <span className="block text-[10px] text-red-400 uppercase">Current (AI)</span>
                                                <span className="text-sm font-mono text-red-300 line-through decoration-red-500/50">{request.currentValue}</span>
                                            </div>
                                            <ArrowRight className="w-4 h-4 text-slate-600" />
                                            <div className="bg-emerald-900/10 px-3 py-2 rounded border border-emerald-900/30">
                                                <span className="block text-[10px] text-emerald-400 uppercase">Proposed</span>
                                                <span className="text-sm font-mono text-emerald-300 font-bold">{request.newValue}</span>
                                            </div>
                                        </div>
                                    ) : (
                                        <div className="flex items-center gap-3 mt-2">
                                            <div className={`p-2 rounded ${request.sourceType === 'URL' ? 'bg-blue-500/10 text-blue-400' : 'bg-red-500/10 text-red-400'}`}>
                                                {request.sourceType === 'URL' ? <LinkIcon className="w-5 h-5" /> : <FileText className="w-5 h-5" />}
                                            </div>
                                            <div>
                                                <p className="text-sm font-medium text-indigo-300 hover:underline cursor-pointer flex items-center gap-1">
                                                    {request.sourceName} <ArrowRight className="w-3 h-3" />
                                                </p>
                                                <div className="flex gap-2 mt-1">
                                                    {request.sourceTags?.map(tag => (
                                                        <span key={tag} className="text-[10px] bg-slate-800 text-slate-400 px-1.5 py-0.5 rounded-full border border-slate-700">{tag}</span>
                                                    ))}
                                                </div>
                                            </div>
                                        </div>
                                    )}
                                </div>

                                <div className="bg-slate-800/50 p-3 rounded border border-slate-800/50">
                                    <div className="flex items-center gap-2 mb-1">
                                        <div className="w-5 h-5 rounded-full bg-indigo-500/20 text-indigo-300 flex items-center justify-center text-xs font-bold">
                                            {request.submittedBy.charAt(0)}
                                        </div>
                                        <span className="text-xs font-semibold text-slate-400">{request.submittedBy}</span>
                                    </div>
                                    <p className="text-sm text-slate-300 italic">"{request.justification}"</p>
                                </div>
                            </div>

                            {/* Right: Actions */}
                            <div className="flex flex-col gap-2 ml-6">
                                <button 
                                    onClick={() => handleApprove(request.id)}
                                    className="flex items-center gap-2 px-4 py-2 bg-emerald-600 hover:bg-emerald-500 text-white rounded-lg text-sm font-medium transition-colors shadow-lg shadow-emerald-900/20 w-40 justify-center"
                                >
                                    <CheckCircle2 className="w-4 h-4" />
                                    {request.type === 'SOURCE' ? 'Approve & Run' : 'Approve'}
                                </button>
                                <button 
                                    onClick={() => setRejectModalOpen(request.id)}
                                    className="flex items-center gap-2 px-4 py-2 bg-slate-800 hover:bg-red-900/30 hover:text-red-400 hover:border-red-900/50 border border-transparent text-slate-400 rounded-lg text-sm font-medium transition-colors w-40 justify-center"
                                >
                                    <XCircle className="w-4 h-4" /> Reject
                                </button>
                            </div>

                        </div>
                    </div>
                ))
            )}
        </div>

      </main>

      {/* Rejection Modal */}
      {rejectModalOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
            <div className="absolute inset-0 bg-slate-950/80 backdrop-blur-sm" onClick={() => setRejectModalOpen(null)} />
            <div className="relative w-full max-w-md bg-slate-900 border border-slate-800 rounded-xl shadow-2xl p-6">
                <h3 className="text-lg font-semibold text-white mb-4 flex items-center gap-2">
                    <AlertTriangle className="w-5 h-5 text-red-500" /> Reject Request
                </h3>
                <p className="text-sm text-slate-400 mb-4">
                    Please provide a reason for rejecting this request. This will be sent back to the analyst.
                </p>
                <textarea 
                    value={rejectionReason}
                    onChange={(e) => setRejectionReason(e.target.value)}
                    className="w-full bg-slate-950 border border-slate-700 rounded-lg p-3 text-slate-200 text-sm focus:border-red-500 focus:outline-none min-h-[100px] mb-4 resize-none"
                    placeholder="Reason for rejection..."
                    autoFocus
                />
                <div className="flex gap-3 justify-end">
                    <button 
                        onClick={() => setRejectModalOpen(null)}
                        className="px-4 py-2 text-slate-400 hover:text-white text-sm font-medium"
                    >
                        Cancel
                    </button>
                    <button 
                        onClick={handleRejectSubmit}
                        disabled={!rejectionReason}
                        className="px-4 py-2 bg-red-600 hover:bg-red-500 text-white rounded-lg text-sm font-medium disabled:opacity-50"
                    >
                        Confirm Rejection
                    </button>
                </div>
            </div>
        </div>
      )}

    </div>
  );
};

export default ApprovalInbox;

import React, { useState, useEffect, useRef } from 'react';
import { Search, Globe, CheckCircle2, AlertCircle, ArrowRight, Loader2, X, Building, ExternalLink, PenLine, CheckCircle, XCircle, Clock, Activity } from 'lucide-react';
import * as api from '../apiService';
import { CompanySummary } from '../types';

interface AddCompanyModalProps {
  isOpen: boolean;
  onClose: () => void;
  onAdd: (company: CompanySummary) => void;
}

// Steps: 0: Search, 'manual': Manual Entry, 1: Confirm, 2: Config, 3: Success/Trigger
type Step = 0 | 'manual' | 1 | 2 | 3;

const AddCompanyModal: React.FC<AddCompanyModalProps> = ({ isOpen, onClose, onAdd }) => {
  const [step, setStep] = useState<Step>(0);
  const [searchQuery, setSearchQuery] = useState('');
  const [isSearching, setIsSearching] = useState(false);
  const [searchResults, setSearchResults] = useState<any[]>([]);
  const [selectedEntity, setSelectedEntity] = useState<any>(null);
  
  // Config state
  const [selectedRegion, setSelectedRegion] = useState('NA');
  const [selectedSector, setSelectedSector] = useState('Technology');
  const [selectedFY, setSelectedFY] = useState('FY2025');
  const [error, setError] = useState<string | null>(null);
  const [totalResults, setTotalResults] = useState<number>(0);
  const [jurisdictionFilter, setJurisdictionFilter] = useState('');

  // Manual entry state
  const [manualName, setManualName] = useState('');
  const [manualLei, setManualLei] = useState('');
  const [manualAddress, setManualAddress] = useState('');
  const [manualJurisdiction, setManualJurisdiction] = useState('');
  const [manualNseSymbol, setManualNseSymbol] = useState('');

  // NSE symbol — can be set in manual entry or overridden in confirmation step
  const [nseSymbol, setNseSymbol] = useState('');
  const [detectedExchange, setDetectedExchange] = useState('');
  const [isLookingUpNse, setIsLookingUpNse] = useState(false);

  const _autoLookupNseSymbol = async (companyName: string) => {
    try {
      setIsLookingUpNse(true);
      setDetectedExchange('');
      const res = await fetch(`/api/search/nse-symbol?q=${encodeURIComponent(companyName)}`);
      if (res.ok) {
        const data = await res.json();
        if (data.symbol) {
          setNseSymbol(data.symbol);
          setDetectedExchange(data.exchange || '');
        }
      }
    } catch { /* silent fail */ } finally {
      setIsLookingUpNse(false);
    }
  };

  // Step 3 — real job tracking
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [createdCompany, setCreatedCompany] = useState<CompanySummary | null>(null);
  const [jobIds, setJobIds] = useState<string[]>([]);
  const [jobStatuses, setJobStatuses] = useState<Record<string, string>>({});
  const [apiError, setApiError] = useState<string | null>(null);
  const pollingRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Poll job statuses when jobIds changes
  useEffect(() => {
    if (jobIds.length === 0) return;
    const poll = async () => {
      const updates: Record<string, string> = {};
      for (const id of jobIds) {
        try {
          const job = await api.getPipelineJobStatus(id);
          updates[id] = job.status;
        } catch { /* ignore */ }
      }
      setJobStatuses(prev => ({ ...prev, ...updates }));
      const allDone = Object.values({ ...jobStatuses, ...updates }).every(s => s === 'PUBLISHED' || s === 'ERROR');
      if (allDone && pollingRef.current) {
        clearInterval(pollingRef.current);
        pollingRef.current = null;
      }
    };
    poll();
    pollingRef.current = setInterval(poll, 3000);
    return () => {
      if (pollingRef.current) clearInterval(pollingRef.current);
    };
  }, [jobIds]);

  if (!isOpen) return null;

  // Map country code → region
  const _countryToRegion = (code: string): string => {
    const c = code.toUpperCase();
    if (['US','CA','MX'].includes(c)) return 'NA';
    if (['GB','DE','FR','NL','ES','IT','SE','CH','BE','NO','DK','FI','PL','PT','AT','IE'].includes(c)) return 'EU';
    if (['IN','CN','JP','AU','SG','HK','KR','MY','TH','ID','NZ','PH'].includes(c)) return 'APAC';
    if (['BR','AR','CL','CO','PE'].includes(c)) return 'LATAM';
    return 'EMEA';
  };


  const handleSearch = async () => {
    if (!searchQuery.trim()) return;
    setIsSearching(true);
    setError(null);
    setSearchResults([]);
    setTotalResults(0);

    try {
      const q = encodeURIComponent(searchQuery.trim());
      // GLEIF — free, no API key, global LEI registry
      const res = await fetch(`/api/search/companies?q=${q}&per_page=20`);
      const body = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(body?.detail ?? `Search failed (${res.status})`);

      const companies = body?.results?.companies ?? [];
      setTotalResults(body?.results?.total_count ?? companies.length);

      if (companies.length === 0) { setError('no_results'); return; }

      setSearchResults(companies.map((c: any) => ({
        name: c.name,
        lei: c.lei ?? '',
        address: c.address || '—',
        ticker: '',
        region: c.region || 'APAC',
        sector: c.sector || 'Unknown',
        gleif_url: c.gleif_url || '',
        incorporation_date: c.incorporation_date || '',
        company_number: c.company_number || '',
        jurisdiction: c.jurisdiction || '',
        current_status: c.current_status || '',
        legal_form: c.legal_form || '',
        registry_authority: c.registry_authority || '',
      })));
    } catch (err: any) {
      setError(err.message || 'Failed to search. Please try again.');
    } finally {
      setIsSearching(false);
    }
  };

  const handleTrigger = async () => {
    setStep(3);
    setIsSubmitting(true);
    setApiError(null);
    try {
      // 1. Create company in DB
      const created = await api.addCompany({
        name: selectedEntity.name,
        lei: selectedEntity.lei || undefined,
        ticker: nseSymbol.trim() || selectedEntity.ticker || undefined,
        region: selectedRegion,
        sector: selectedSector,
      });
      setCreatedCompany(created);
      onAdd(created);

      // 2. Immediately kick off pipeline
      const currentYear = new Date().getFullYear();
      const jobs = await api.runPipeline({
        company_ids: [created.id],
        data_sources: ['Secondary'],
        financial_years: [selectedFY || `FY${currentYear}`],
      });
      const ids = jobs.map(j => j.id);
      setJobIds(ids);
      const initStatuses: Record<string, string> = {};
      jobs.forEach(j => { initStatuses[j.id] = j.status; });
      setJobStatuses(initStatuses);
    } catch (err: any) {
      setApiError(err.message ?? 'Something went wrong');
    } finally {
      setIsSubmitting(false);
    }
  };

  const renderStepContent = () => {
    switch(step) {
      case 0: // Global Search
        return (
          <div className="space-y-4">
            <div className="flex gap-2">
              <div className="relative flex-1">
                <Search className="absolute left-3 top-3 w-5 h-5 text-slate-500" />
                <input
                  type="text"
                  placeholder="Search by legal name…"
                  className="w-full bg-slate-800 border border-slate-700 rounded-lg py-3 pl-10 pr-4 text-slate-200 focus:border-indigo-500 focus:outline-none"
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  onKeyDown={(e) => e.key === 'Enter' && handleSearch()}
                />
              </div>
              <button
                onClick={handleSearch}
                className="bg-indigo-600 hover:bg-indigo-500 text-white px-4 py-3 rounded-lg text-sm font-medium transition-colors shrink-0"
              >
                Search
              </button>
            </div>
            
            <div className="min-h-[200px]">
              {isSearching ? (
                <div className="flex flex-col items-center justify-center h-40 text-slate-500">
                  <Loader2 className="w-8 h-8 animate-spin mb-2" />
                  <span className="text-sm">Searching GLEIF Global Registry…</span>
                </div>
              ) : error === 'no_results' ? (
                <div className="flex flex-col items-center justify-center h-40 text-slate-400 text-center px-4 gap-3">
                  <AlertCircle className="w-8 h-8 text-amber-500" />
                  <div>
                    <p className="text-sm font-medium text-slate-300">No companies found</p>
                    <p className="text-xs text-slate-500 mt-1">GLEIF covers LEI-registered entities. Try a different spelling, or add the company manually.</p>
                  </div>
                  <button
                    onClick={() => { setManualName(searchQuery); setStep('manual'); }}
                    className="flex items-center gap-2 bg-indigo-600 hover:bg-indigo-500 text-white text-sm px-4 py-2 rounded-lg transition-colors"
                  >
                    <PenLine className="w-4 h-4" /> Add Company Manually
                  </button>
                </div>
              ) : error ? (
                <div className="flex flex-col items-center justify-center h-40 text-red-400 text-center px-4 gap-3">
                  <AlertCircle className="w-8 h-8" />
                  <p className="text-sm">{error}</p>
                  <button onClick={handleSearch} className="text-xs text-indigo-400 hover:underline">Try again</button>
                </div>
              ) : searchResults.length > 0 ? (
                <div className="space-y-3">
                  <div className="flex justify-between items-center">
                    <p className="text-xs text-slate-500 uppercase font-semibold">Found {totalResults} {totalResults === 1 ? 'entity' : 'entities'}</p>
                    <a href="https://gleif.org" target="_blank" rel="noopener noreferrer" className="text-[10px] text-slate-600 hover:text-indigo-400 flex items-center gap-1 transition-colors"><Globe className="w-2.5 h-2.5" /> Powered by GLEIF</a>
                  </div>
                  <div className="space-y-2 max-h-64 overflow-y-auto pr-1">
                    {searchResults.map((result, idx) => (
                      <div
                        key={idx}
                        onClick={() => {
                          setSelectedEntity(result);
                          setSelectedRegion(result.region || 'NA');
                          setSelectedSector(result.sector || 'Technology');
                          setNseSymbol('');
                          _autoLookupNseSymbol(result.name);
                          setStep(1);
                        }}
                        className="p-3 bg-slate-800/50 border border-slate-700 hover:border-indigo-500 hover:bg-indigo-500/10 rounded-lg cursor-pointer transition-all group"
                      >
                        <div className="flex justify-between items-start gap-2">
                          <h4 className="font-medium text-slate-200 group-hover:text-indigo-400 text-sm leading-tight">{result.name}</h4>
                          <div className="flex items-center gap-1.5 shrink-0">
                            {result.current_status && (
                              <span className={`text-[10px] font-bold px-1.5 py-0.5 rounded uppercase ${
                                result.current_status.toLowerCase() === 'active'
                                  ? 'bg-emerald-500/15 text-emerald-400 border border-emerald-500/30'
                                  : result.current_status.toLowerCase() === 'inactive' || result.current_status.toLowerCase() === 'lapsed'
                                  ? 'bg-red-500/15 text-red-400 border border-red-500/30'
                                  : 'bg-slate-700 text-slate-400 border border-slate-600'
                              }`}>{result.current_status}</span>
                            )}
                            <ArrowRight className="w-4 h-4 text-slate-600 group-hover:text-indigo-400" />
                          </div>
                        </div>
                        <div className="flex items-center gap-2 mt-1 flex-wrap">
                          {result.lei && <span className="text-[10px] font-mono bg-slate-900 px-1.5 py-0.5 rounded text-indigo-400/80 border border-slate-700">LEI: {result.lei}</span>}
                          {result.company_number && <span className="text-[10px] font-mono bg-slate-900 px-1.5 py-0.5 rounded text-slate-500 border border-slate-700">Reg #{result.company_number}</span>}
                          {result.jurisdiction && <span className="text-[10px] font-mono bg-slate-900 px-1.5 py-0.5 rounded text-slate-500 border border-slate-700">{result.jurisdiction.toUpperCase()}</span>}
                          {result.address && result.address !== '—' && <span className="text-[10px] text-slate-500 truncate max-w-[180px]">{result.address}</span>}
                        </div>
                        <div className="flex items-center gap-3 mt-1">
                          {result.incorporation_date && <p className="text-[10px] text-slate-600">Est. {result.incorporation_date}</p>}
                          {result.legal_form && <p className="text-[10px] text-slate-600">{result.legal_form}</p>}
                          {result.gleif_url && (
                            <a
                              href={result.gleif_url}
                              target="_blank" rel="noopener noreferrer"
                              onClick={(e) => e.stopPropagation()}
                              className="text-[10px] text-indigo-500 hover:text-indigo-400 flex items-center gap-0.5 transition-colors"
                            >
                              <ExternalLink className="w-2.5 h-2.5" /> View on GLEIF
                            </a>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                  {totalResults > searchResults.length && (
                    <p className="text-[10px] text-slate-600 text-center">Showing {searchResults.length} of {totalResults.toLocaleString()} results. Add a country code to narrow results.</p>
                  )}
                  <button
                    onClick={() => { setManualName(searchQuery); setStep('manual'); }}
                    className="w-full flex items-center justify-center gap-1.5 text-xs text-slate-500 hover:text-indigo-400 transition-colors border border-dashed border-slate-700 hover:border-indigo-500 px-3 py-2 rounded-lg"
                  >
                    <PenLine className="w-3.5 h-3.5" /> Can't find your company? Add manually
                  </button>
                </div>
              ) : (
                <div className="flex flex-col items-center justify-center h-40 text-slate-600 gap-3">
                  <Globe className="w-12 h-12 opacity-20" />
                  <p className="text-sm text-center">Search <span className="text-slate-400 font-medium">GLEIF Global Registry</span> — verified legal entities with LEI numbers worldwide.</p>
                  <p className="text-[10px] text-slate-600 text-center">Covers 2.4M+ regulated entities · Free · No API key needed</p>
                  <button
                    onClick={() => setStep('manual')}
                    className="flex items-center gap-1.5 text-xs text-slate-500 hover:text-indigo-400 transition-colors border border-slate-700 hover:border-indigo-500 px-3 py-1.5 rounded-lg"
                  >
                    <PenLine className="w-3.5 h-3.5" /> Or add company manually
                  </button>
                </div>
              )}
            </div>
          </div>
        );
      
      case 'manual': // Manual Entry
        return (
          <div className="space-y-4">
            <p className="text-xs text-slate-500">Enter the company details manually. The LEI code is optional but recommended for verified entities.</p>
            <div>
              <label className="block text-xs text-slate-400 mb-1">Legal Name <span className="text-red-400">*</span></label>
              <input
                type="text"
                placeholder="e.g. Impactree Data Technologies Private Limited"
                className="w-full bg-slate-800 border border-slate-700 rounded-lg py-2.5 px-3 text-slate-200 focus:border-indigo-500 focus:outline-none text-sm"
                value={manualName}
                onChange={(e) => setManualName(e.target.value)}
              />
            </div>
            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="block text-xs text-slate-400 mb-1">LEI Code <span className="text-slate-600">(optional)</span></label>
                <input
                  type="text"
                  placeholder="20-char LEI code"
                  className="w-full bg-slate-800 border border-slate-700 rounded-lg py-2.5 px-3 text-slate-200 focus:border-indigo-500 focus:outline-none text-sm font-mono"
                  value={manualLei}
                  onChange={(e) => setManualLei(e.target.value.toUpperCase())}
                  maxLength={20}
                />
              </div>
              <div>
                <label className="block text-xs text-slate-400 mb-1">Country Code</label>
                <input
                  type="text"
                  placeholder="e.g. IN, US, GB"
                  className="w-full bg-slate-800 border border-slate-700 rounded-lg py-2.5 px-3 text-slate-200 focus:border-indigo-500 focus:outline-none text-sm uppercase"
                  value={manualJurisdiction}
                  onChange={(e) => setManualJurisdiction(e.target.value.toUpperCase())}
                  maxLength={2}
                />
              </div>
            </div>
            <div>
              <label className="block text-xs text-slate-400 mb-1">Address <span className="text-slate-600">(optional)</span></label>
              <input
                type="text"
                placeholder="City, Country"
                className="w-full bg-slate-800 border border-slate-700 rounded-lg py-2.5 px-3 text-slate-200 focus:border-indigo-500 focus:outline-none text-sm"
                value={manualAddress}
                onChange={(e) => setManualAddress(e.target.value)}
              />
            </div>
            <div>
              <label className="block text-xs text-slate-400 mb-1">NSE Symbol <span className="text-slate-600">(optional — enables real Annual Report scraping)</span></label>
              <input
                type="text"
                placeholder="e.g. TATAMOTORS, HCLTECH, INFY"
                className="w-full bg-slate-800 border border-slate-700 rounded-lg py-2.5 px-3 text-slate-200 focus:border-indigo-500 focus:outline-none text-sm uppercase font-mono"
                value={manualNseSymbol}
                onChange={(e) => setManualNseSymbol(e.target.value.toUpperCase())}
              />
            </div>
          </div>
        );

      case 1: // Confirmation
        return (
          <div className="space-y-6 text-center py-4">
             <div className="w-16 h-16 bg-indigo-500/20 rounded-full flex items-center justify-center mx-auto text-indigo-400">
                <Building className="w-8 h-8" />
             </div>
             <div>
                <h3 className="text-xl font-semibold text-white">{selectedEntity.name}</h3>
                <p className="text-slate-400 text-sm mt-1">{selectedEntity.address}</p>
                <div className="flex items-center justify-center gap-2 mt-3 flex-wrap">
                  {selectedEntity.lei && (
                    <span className="inline-block bg-slate-900 border border-indigo-500/30 rounded px-2.5 py-1 text-xs font-mono text-indigo-400">
                      LEI: {selectedEntity.lei}
                    </span>
                  )}
                  {selectedEntity.company_number && (
                    <span className="inline-block bg-slate-900 border border-slate-700 rounded px-2.5 py-1 text-xs font-mono text-slate-300">
                      Reg #{selectedEntity.company_number}
                    </span>
                  )}
                  {selectedEntity.jurisdiction && (
                    <span className="inline-block bg-slate-900 border border-slate-700 rounded px-2.5 py-1 text-xs font-mono text-slate-400">
                      {selectedEntity.jurisdiction.toUpperCase()}
                    </span>
                  )}
                  {selectedEntity.current_status && (
                    <span className={`inline-block rounded px-2.5 py-1 text-xs font-bold border ${
                      selectedEntity.current_status.toLowerCase() === 'active'
                        ? 'bg-emerald-500/15 text-emerald-400 border-emerald-500/30'
                        : selectedEntity.current_status.toLowerCase() === 'inactive' || selectedEntity.current_status.toLowerCase() === 'lapsed'
                        ? 'bg-red-500/15 text-red-400 border-red-500/30'
                        : 'bg-slate-700 text-slate-400 border-slate-600'
                    }`}>{selectedEntity.current_status}</span>
                  )}
                </div>
                {selectedEntity.gleif_url && (
                  <a
                    href={selectedEntity.gleif_url}
                    target="_blank" rel="noopener noreferrer"
                    className="inline-flex items-center gap-1 mt-3 text-xs text-indigo-400 hover:text-indigo-300 transition-colors"
                  >
                    <ExternalLink className="w-3 h-3" /> View on GLEIF
                  </a>
                )}
             </div>
             <p className="text-sm text-slate-400 max-w-sm mx-auto">
                Is this the correct legal entity you wish to ingest? This will trigger an ESG data scrape.
             </p>
             <div className="mt-4 w-full border-t border-slate-800 pt-4 text-left">
               <label className="block text-xs text-slate-500 mb-1 flex items-center gap-2">
                 Stock Ticker / Symbol
                 {isLookingUpNse && <span className="inline-flex items-center gap-1 text-indigo-400"><Loader2 className="w-3 h-3 animate-spin" /> auto-detecting…</span>}
                 {!isLookingUpNse && nseSymbol && (
                   <span className="text-emerald-400 text-[10px] font-bold">
                     ✓ {detectedExchange ? `found on ${detectedExchange}` : 'auto-detected'}
                   </span>
                 )}
                 {!isLookingUpNse && !nseSymbol && <span className="text-slate-600">(optional — enables real Annual Report scraping for any listed company)</span>}
               </label>
               <input
                 type="text"
                 placeholder="e.g. TATAMOTORS (NSE), AAPL (NASDAQ), SHEL (NYSE)"
                 className="w-full bg-slate-800 border border-slate-700 rounded-lg py-2 px-3 text-slate-200 focus:border-indigo-500 focus:outline-none text-sm uppercase font-mono"
                 value={nseSymbol}
                 onChange={(e) => setNseSymbol(e.target.value.toUpperCase())}
               />
             </div>
          </div>
        );

      case 2: // Configuration
        return (
          <div className="space-y-4">
            <div>
               <label className="block text-xs text-slate-500 uppercase font-semibold mb-2">Primary Operating Region</label>
               <div className="grid grid-cols-2 gap-2">
                  {['NA', 'EU', 'APAC', 'LATAM'].map(region => (
                    <button
                        key={region}
                        onClick={() => setSelectedRegion(region)}
                        className={`p-3 rounded border text-sm font-medium transition-all ${
                            selectedRegion === region 
                            ? 'bg-indigo-600 text-white border-indigo-600' 
                            : 'bg-slate-800 border-slate-700 text-slate-400 hover:border-slate-500'
                        }`}
                    >
                        {region}
                    </button>
                  ))}
               </div>
            </div>

            <div>
               <label className="block text-xs text-slate-500 uppercase font-semibold mb-2">Sector Classification</label>
               <select 
                  className="w-full bg-slate-800 border border-slate-700 rounded p-3 text-slate-200 focus:border-indigo-500 outline-none appearance-none"
                  value={selectedSector}
                  onChange={(e) => setSelectedSector(e.target.value)}
               >
                   <option>Technology</option>
                   <option>Materials / Steel</option>
                   <option>Energy / Oil & Gas</option>
                   <option>Finance</option>
                   <option>Consumer Goods</option>
               </select>
            </div>

            <div className="bg-amber-500/10 border border-amber-500/20 p-3 rounded flex gap-3 items-start">
               <AlertCircle className="w-5 h-5 text-amber-500 shrink-0 mt-0.5" />
               <p className="text-xs text-amber-200">
                  Selecting the correct region optimizes the scraper for local regulatory filings (e.g., CSRD in EU, SEBI in India).
               </p>
            </div>

            <div>
               <label className="block text-xs text-slate-500 uppercase font-semibold mb-2">Financial Year</label>
               <select
                  className="w-full bg-slate-800 border border-slate-700 rounded p-3 text-slate-200 focus:border-indigo-500 outline-none appearance-none"
                  value={selectedFY}
                  onChange={(e) => setSelectedFY(e.target.value)}
               >
                  {Array.from({ length: 10 }, (_, i) => `FY${new Date().getFullYear() - i}`).map(fy => (
                    <option key={fy}>{fy}</option>
                  ))}
               </select>
            </div>
          </div>
        );

      case 3: // Processing / Done
         if (apiError) {
           return (
             <div className="flex flex-col items-center justify-center py-10 space-y-4 text-center">
               <XCircle className="w-12 h-12 text-red-500" />
               <div>
                 <h3 className="text-lg font-medium text-white">Something went wrong</h3>
                 <p className="text-slate-400 text-sm mt-1">{apiError}</p>
               </div>
               <button onClick={() => { setStep(2); setApiError(null); }} className="text-xs text-indigo-400 hover:underline">← Go back and try again</button>
             </div>
           );
         }

         if (isSubmitting) {
           return (
             <div className="flex flex-col items-center justify-center py-10 space-y-4">
               <Loader2 className="w-12 h-12 text-indigo-500 animate-spin" />
               <div className="text-center">
                 <h3 className="text-lg font-medium text-white">Creating Company & Queuing Pipeline</h3>
                 <p className="text-slate-400 text-sm">Registering in database and allocating scrapers…</p>
               </div>
             </div>
           );
         }

         const allDone = jobIds.length > 0 && jobIds.every(id => jobStatuses[id] === 'PUBLISHED' || jobStatuses[id] === 'ERROR');
         const statusIcon = (s: string) => {
           if (s === 'PUBLISHED') return <CheckCircle className="w-4 h-4 text-emerald-400" />;
           if (s === 'ERROR') return <XCircle className="w-4 h-4 text-red-400" />;
           if (s === 'FETCHING') return <Activity className="w-4 h-4 text-indigo-400 animate-pulse" />;
           return <Clock className="w-4 h-4 text-slate-400 animate-pulse" />;
         };
         const statusColor = (s: string) => {
           if (s === 'PUBLISHED') return 'text-emerald-400';
           if (s === 'ERROR') return 'text-red-400';
           if (s === 'FETCHING') return 'text-indigo-400';
           return 'text-slate-400';
         };

         return (
           <div className="space-y-6 py-4">
             {/* Company created */}
             <div className="flex items-center gap-4 bg-emerald-500/10 border border-emerald-500/20 rounded-xl p-4">
               <CheckCircle2 className="w-8 h-8 text-emerald-400 shrink-0" />
               <div>
                 <p className="text-sm font-bold text-white">{createdCompany?.name}</p>
                 {createdCompany?.lei && <p className="text-xs font-mono text-slate-400 mt-0.5">LEI: {createdCompany.lei}</p>}
                 <p className="text-xs text-emerald-400 mt-0.5">Company registered successfully</p>
               </div>
             </div>

             {/* Job status */}
             <div>
               <p className="text-xs text-slate-500 uppercase font-bold tracking-wider mb-3 flex items-center gap-2">
                 <Activity className="w-3.5 h-3.5" /> Pipeline Jobs
               </p>
               {jobIds.length === 0 ? (
                 <div className="flex items-center gap-2 text-slate-400 text-sm">
                   <Loader2 className="w-4 h-4 animate-spin" /> Starting pipeline…
                 </div>
               ) : (
                 <div className="space-y-2">
                   {jobIds.map(id => (
                     <div key={id} className="flex items-center justify-between bg-slate-800 border border-slate-700 rounded-lg px-4 py-3">
                       <div className="flex items-center gap-2">
                         {statusIcon(jobStatuses[id] ?? 'QUEUED')}
                         <span className="text-sm text-slate-300 font-mono">Job #{id}</span>
                       </div>
                       <span className={`text-xs font-bold uppercase ${statusColor(jobStatuses[id] ?? 'QUEUED')}`}>
                         {jobStatuses[id] ?? 'QUEUED'}
                       </span>
                     </div>
                   ))}
                   {!allDone && (
                     <p className="text-xs text-slate-500 text-center mt-2 flex items-center justify-center gap-1">
                       <Loader2 className="w-3 h-3 animate-spin" /> Polling every 3s…
                     </p>
                   )}
                 </div>
               )}
             </div>
           </div>
         );
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      {/* Backdrop */}
      <div className="absolute inset-0 bg-slate-950/80 backdrop-blur-sm" onClick={onClose} />
      
      {/* Modal Content */}
      <div className="relative bg-slate-900 border border-slate-800 rounded-xl shadow-2xl w-full max-w-lg overflow-hidden flex flex-col max-h-[90vh]">
        {/* Header */}
        <div className="px-6 py-4 border-b border-slate-800 flex justify-between items-center bg-slate-900">
          <div>
            <h2 className="text-lg font-semibold text-white">Add Company</h2>
            <p className="text-xs text-slate-500">Step {step + 1} of 3</p>
          </div>
          <button onClick={onClose} className="text-slate-500 hover:text-white transition-colors">
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Body */}
        <div className="p-6 flex-1 overflow-y-auto">
          {renderStepContent()}
        </div>

        {/* Footer */}
        {step === 3 ? (
          !isSubmitting && !apiError && (
            <div className="px-6 py-4 border-t border-slate-800 bg-slate-900/50 flex justify-end">
              <button
                onClick={onClose}
                className="bg-indigo-600 hover:bg-indigo-500 text-white px-6 py-2 rounded-lg text-sm font-medium flex items-center gap-2 transition-colors"
              >
                <CheckCircle className="w-4 h-4" /> Close
              </button>
            </div>
          )
        ) : (
            <div className="px-6 py-4 border-t border-slate-800 bg-slate-900/50 flex justify-between">
                {step !== 0 ? (
                    <button 
                        onClick={() => {
                          if (step === 'manual') setStep(0);
                          else if (step === 1) setStep(selectedEntity?._manual ? 'manual' : 0);
                          else setStep(prev => (prev as number) - 1 as Step);
                        }}
                        className="px-4 py-2 text-sm font-medium text-slate-400 hover:text-white transition-colors"
                    >
                        Back
                    </button>
                ) : <div></div>}
                
                {step === 0 && (
                   <a href="https://gleif.org" target="_blank" rel="noopener noreferrer" className="text-xs text-slate-600 hover:text-indigo-400 flex items-center gap-1 transition-colors">
                      <Globe className="w-3 h-3" /> Powered by GLEIF
                   </a>
                )}

                {step === 'manual' && (
                    <button
                        onClick={() => {
                          if (!manualName.trim()) return;
                          setSelectedEntity({
                            name: manualName.trim(),
                            lei: manualLei.trim(),
                            address: manualAddress.trim() || '—',
                            ticker: manualNseSymbol.trim().toUpperCase(),
                            region: _countryToRegion(manualJurisdiction),
                            sector: 'Unknown',
                            jurisdiction: manualJurisdiction,
                            company_number: manualLei.trim(),
                            incorporation_date: '',
                            status: 'ACTIVE',
                            _manual: true,
                          });
                          setNseSymbol(manualNseSymbol.trim().toUpperCase());
                          setSelectedRegion(_countryToRegion(manualJurisdiction));
                          setStep(1);
                        }}
                        disabled={!manualName.trim()}
                        className="bg-indigo-600 hover:bg-indigo-500 disabled:opacity-40 disabled:cursor-not-allowed text-white px-6 py-2 rounded-lg text-sm font-medium flex items-center gap-2 transition-colors"
                    >
                        Continue <ArrowRight className="w-4 h-4" />
                    </button>
                )}

                {step === 1 && (
                    <button 
                        onClick={() => setStep(2)}
                        className="bg-indigo-600 hover:bg-indigo-500 text-white px-6 py-2 rounded-lg text-sm font-medium flex items-center gap-2 transition-colors"
                    >
                        Confirm Identity <ArrowRight className="w-4 h-4" />
                    </button>
                )}
                
                {step === 2 && (
                    <button 
                        onClick={handleTrigger}
                        className="bg-emerald-600 hover:bg-emerald-500 text-white px-6 py-2 rounded-lg text-sm font-medium flex items-center gap-2 transition-colors shadow-lg shadow-emerald-900/20"
                    >
                        <CheckCircle2 className="w-4 h-4" /> Start Ingestion
                    </button>
                )}
            </div>
        )}
      </div>
    </div>
  );
};

export default AddCompanyModal;
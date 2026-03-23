import { CompanyData, CompanySummary } from './types';

export const MOCK_UNIVERSE: CompanySummary[] = [
  {
    id: 'c-123',
    name: 'Tata Steel Ltd.',
    ticker: 'TATASTEEL',
    lei: '335800U45W3452H234',
    region: 'APAC',
    sector: 'Materials / Steel',
    status: 'PUBLISHED',
    riskScores: { s: 42, p: 88, o: 65, f: 35 },
    financialYear: 'FY2024',
    lastUpdated: '2 hours ago'
  },
  {
    id: 'c-124',
    name: 'Tesla Inc.',
    ticker: 'TSLA',
    lei: '5493006MNBJD5Z0A32',
    region: 'NA',
    sector: 'Auto / EV',
    status: 'FETCHING',
    riskScores: { s: 0, p: 0, o: 0, f: 0 },
    financialYear: 'FY2024',
    lastUpdated: 'Just now'
  },
  {
    id: 'c-125',
    name: 'Orsted A/S',
    ticker: 'ORSTED',
    lei: '2138006MNBJD5Z0A32',
    region: 'EU',
    sector: 'Energy',
    status: 'NEEDS_REVIEW',
    riskScores: { s: 15, p: 20, o: 45, f: 60 },
    financialYear: 'FY2023',
    lastUpdated: '1 day ago'
  },
  {
    id: 'c-126',
    name: 'Evergreen Marine',
    ticker: '2603',
    lei: '9845006MNBJD5Z0A32',
    region: 'APAC',
    sector: 'Transportation',
    status: 'ERROR',
    riskScores: { s: 0, p: 0, o: 0, f: 0 },
    financialYear: 'FY2023',
    lastUpdated: '5 hours ago'
  },
  {
    id: 'c-127',
    name: 'Rio Tinto Group',
    ticker: 'RIO',
    lei: '529900I912030201',
    region: 'EU',
    sector: 'Materials / Mining',
    status: 'PUBLISHED',
    riskScores: { s: 68, p: 55, o: 82, f: 15 },
    financialYear: 'FY2024',
    lastUpdated: '30 mins ago'
  },
  {
    id: 'c-128',
    name: 'Petrobras',
    ticker: 'PBR',
    lei: '5493006MNBJD5Z0A99',
    region: 'LATAM',
    sector: 'Energy / Oil',
    status: 'SCORING',
    riskScores: { s: 85, p: 40, o: 50, f: 60 },
    financialYear: 'FY2024',
    lastUpdated: '10 mins ago'
  }
];

export const MOCK_COMPANY: CompanyData = {
  id: 'c-123',
  name: 'Tata Steel Ltd.',
  ticker: 'TATASTEEL',
  lei: '335800U45W3452H234',
  sector: 'Materials / Steel',
  financialYear: 'FY2024',
  status: 'LIVE',
  lastUpdated: '2 hours ago',
  version: 'v2024.05.12',
  pillars: {
    sustainability: {
      id: 'sus',
      name: 'Sustainability Risk',
      score: 42,
      trend: 'down',
      trendValue: 2.5,
      drivers: [
        { id: 'd1', name: 'Scope 1 Emissions', impact: 80 },
        { id: 'd2', name: 'Water Usage Intensity', impact: 60 },
        { id: 'd3', name: 'Waste Recycling', impact: 40 },
      ]
    },
    pchi: {
      id: 'pchi',
      name: 'PCHI (Climate)',
      score: 88, // High Risk
      trend: 'up',
      trendValue: 5.1,
      drivers: [
        { id: 'd4', name: 'Coastal Flood Risk', impact: 95 },
        { id: 'd5', name: 'Heat Stress (India Ops)', impact: 90 },
        { id: 'd6', name: 'Cyclone Vulnerability', impact: 75 },
      ]
    },
    operational: {
      id: 'ops',
      name: 'Operational Risk',
      score: 65,
      trend: 'stable',
      trendValue: 0,
      drivers: [
        { id: 'd7', name: 'Supply Chain Disruption', impact: 70 },
        { id: 'd8', name: 'Labor Strike History', impact: 50 },
      ]
    },
    financial: {
      id: 'fin',
      name: 'Financial Risk',
      score: 35,
      trend: 'down',
      trendValue: 1.2,
      drivers: [
        { id: 'd9', name: 'Debt/Equity Ratio', impact: 45 },
        { id: 'd10', name: 'Liquidity Coverage', impact: 30 },
      ]
    }
  },
  indicators: [
    { id: 'i1', name: 'Total Scope 1 Emissions', value: 32.5, unit: 'MtCO2e', confidence: 98, source: 'Annual Report 2023', isOverridden: false, lastUpdated: '2024-01-15' },
    { id: 'i2', name: 'Water Withdrawal', value: 15000, unit: 'Megaliters', confidence: 85, source: 'Sustainability Report', isOverridden: true, overrideReason: 'Correction based on Q3 audit', lastUpdated: '2024-02-20' },
    { id: 'i3', name: 'Lost Time Injury Rate', value: 0.45, unit: 'Rate', confidence: 92, source: 'Safety Filing', isOverridden: false, lastUpdated: '2024-03-10' },
    { id: 'i4', name: 'EBITDA Margin', value: 14.2, unit: '%', confidence: 99, source: 'Bloomberg', isOverridden: false, lastUpdated: '2024-05-01' },
    { id: 'i5', name: 'Net Debt', value: 7.2, unit: 'Bn USD', confidence: 70, source: 'AI Inference (News)', isOverridden: false, lastUpdated: '2024-05-10' },
  ],
  evidence: [
    { id: 'e1', type: 'PDF', name: 'Tata Steel Integrated Report 2023-24.pdf', date: '2024-05-01', status: 'processed', tags: ['Annual Report', 'Financials'] },
    { id: 'e2', type: 'NEWS', name: 'Reuters: Blast furnace shut down scheduled', date: '2024-04-15', status: 'processed', tags: ['Operational', 'News'] },
    { id: 'e3', type: 'URL', name: 'https://tatasteel.com/sustainability/water', date: '2024-02-10', status: 'processed', tags: ['ESG', 'Water'] },
  ]
};
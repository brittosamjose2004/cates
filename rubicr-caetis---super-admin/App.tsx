import React, { useState } from 'react';
import { 
  LayoutDashboard, 
  Building2, 
  Settings, 
  LogOut,
  Inbox,
  User as UserIcon,
  Shield
} from 'lucide-react';
import MasterUniverse from './components/MasterUniverse';
import CompanyDetail from './components/CompanyDetail';
import RiskConfiguration from './components/RiskConfiguration';
import ApprovalInbox from './components/ApprovalInbox';
import Login from './components/Login';
import { User } from './types';

type ViewMode = 'DASHBOARD' | 'COMPANY_DETAIL' | 'SETTINGS' | 'APPROVAL_INBOX';

export default function App() {
  const [user, setUser] = useState<User | null>(null);
  const [currentView, setCurrentView] = useState<ViewMode>('DASHBOARD');
  // In a real app, this would be an ID we fetch
  const [selectedCompanyId, setSelectedCompanyId] = useState<string | null>(null);

  const navigateToCompany = (id: string) => {
    setSelectedCompanyId(id);
    setCurrentView('COMPANY_DETAIL');
  };

  const navigateToDashboard = () => {
    setCurrentView('DASHBOARD');
    setSelectedCompanyId(null);
  };

  const navigateToSettings = () => {
    setCurrentView('SETTINGS');
    setSelectedCompanyId(null);
  };

  const navigateToInbox = () => {
    setCurrentView('APPROVAL_INBOX');
    setSelectedCompanyId(null);
  };

  const handleLogout = () => {
    setUser(null);
    setCurrentView('DASHBOARD');
  };

  if (!user) {
    return <Login onLogin={setUser} />;
  }

  return (
    <div className="flex h-screen overflow-hidden bg-slate-900 text-slate-200 font-sans selection:bg-indigo-500/30">
      
      {/* Sidebar - Global Navigation */}
      <aside className="w-20 flex flex-col items-center py-6 border-r border-slate-800 bg-slate-950/50 backdrop-blur-sm z-20">
        <div className="w-10 h-10 bg-indigo-600 rounded-xl flex items-center justify-center mb-10 shadow-lg shadow-indigo-500/20 cursor-pointer hover:scale-105 transition-transform" onClick={navigateToDashboard}>
          <Shield className="w-6 h-6 text-white" />
        </div>
        
        <nav className="flex-1 flex flex-col gap-6">
          <SidebarIcon 
             icon={<LayoutDashboard />} 
             label="Dashboard" 
             active={currentView === 'DASHBOARD'} 
             onClick={navigateToDashboard}
          />
          <SidebarIcon 
             icon={<Building2 />} 
             label="Companies" 
             active={currentView === 'COMPANY_DETAIL'} 
             // Logic to just go back to company list if clicked
             onClick={navigateToDashboard}
          />
          {user.role === 'ADMIN' && (
            <SidebarIcon 
               icon={<Inbox />} 
               label="Approval Inbox" 
               active={currentView === 'APPROVAL_INBOX'} 
               onClick={navigateToInbox}
            />
          )}
          <SidebarIcon 
             icon={<Settings />} 
             label="Settings" 
             active={currentView === 'SETTINGS'} 
             onClick={navigateToSettings}
          />
        </nav>

        <div className="mt-auto flex flex-col gap-6 items-center">
          <div className="group relative">
            <div className="w-10 h-10 rounded-full bg-slate-800 border border-slate-700 flex items-center justify-center text-indigo-400 font-bold cursor-help">
              {user.name.charAt(0)}
            </div>
            <div className="absolute left-14 bottom-0 bg-slate-800 text-xs p-3 rounded-xl opacity-0 group-hover:opacity-100 transition-opacity whitespace-nowrap border border-slate-700 pointer-events-none z-50 shadow-2xl">
              <p className="font-bold text-white">{user.name}</p>
              <p className="text-slate-500 text-[10px] mt-0.5">{user.role}</p>
            </div>
          </div>
          <SidebarIcon icon={<LogOut />} label="Logout" onClick={handleLogout} />
        </div>
      </aside>

      {/* Main Content Area - Swaps based on View */}
      <main className="flex-1 h-full overflow-hidden relative flex flex-col">
          {currentView === 'DASHBOARD' ? (
              <MasterUniverse onNavigateToCompany={navigateToCompany} userRole={user.role} />
          ) : currentView === 'COMPANY_DETAIL' ? (
              <CompanyDetail onBack={navigateToDashboard} userRole={user.role} companyId={selectedCompanyId ?? undefined} />
          ) : currentView === 'APPROVAL_INBOX' ? (
              user.role === 'ADMIN' ? <ApprovalInbox /> : <div className="p-10 text-center text-slate-500">Access Denied</div>
          ) : (
              <RiskConfiguration />
          )}
      </main>
    </div>
  );
}

// Helper for Sidebar Icons
interface SidebarIconProps {
    icon: React.ReactNode;
    label: string;
    active?: boolean;
    onClick?: () => void;
}

const SidebarIcon = ({ icon, label, active = false, onClick }: SidebarIconProps) => (
  <button 
    onClick={onClick}
    className={`
      p-3 rounded-xl transition-all group relative
      ${active ? 'bg-indigo-500/10 text-indigo-400' : 'text-slate-500 hover:bg-slate-800 hover:text-slate-300'}
    `}
  >
    {icon}
    {/* Tooltip */}
    <span className="absolute left-14 bg-slate-800 text-xs px-2 py-1 rounded opacity-0 group-hover:opacity-100 transition-opacity whitespace-nowrap border border-slate-700 pointer-events-none z-50 shadow-lg">
      {label}
    </span>
  </button>
);
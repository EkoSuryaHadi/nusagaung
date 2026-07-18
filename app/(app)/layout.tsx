import Sidebar from "@/components/Sidebar";
import AuthGuard from "@/components/AuthGuard";
import ChatPanel from "./components/ChatPanel";
import { SidebarProvider, MainContent } from "@/components/SidebarProvider";

export default function AppLayout({ children }: { children: React.ReactNode }) {
  return (
    <AuthGuard>
      <SidebarProvider>
        <Sidebar />
        <MainContent>
          {children}
        </MainContent>
      </SidebarProvider>
      <ChatPanel />
    </AuthGuard>
  );
}

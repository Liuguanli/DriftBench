import { Outlet, Link, useLocation } from "react-router";
import { Database, TrendingUp, Wand2, BarChart3, BookOpen, Menu } from "lucide-react";
import { Button } from "./ui/button";
import { Sheet, SheetContent, SheetTrigger } from "./ui/sheet";
import { useState } from "react";

const navigation = [
  { name: "Home", href: "/", icon: Database },
  { name: "Drift Types", href: "/drift-types", icon: TrendingUp },
  { name: "Generator", href: "/generator", icon: Wand2 },
  { name: "Visualization", href: "/visualization", icon: BarChart3 },
  { name: "Case Studies", href: "/case-studies", icon: BookOpen },
];

export function Layout() {
  const location = useLocation();
  const [open, setOpen] = useState(false);

  const NavLinks = () => (
    <>
      {navigation.map((item) => {
        const Icon = item.icon;
        const isActive = location.pathname === item.href;
        return (
          <Link
            key={item.name}
            to={item.href}
            onClick={() => setOpen(false)}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg transition-colors ${
              isActive
                ? "bg-primary text-primary-foreground"
                : "hover:bg-accent hover:text-accent-foreground"
            }`}
          >
            <Icon className="size-5" />
            <span>{item.name}</span>
          </Link>
        );
      })}
    </>
  );

  return (
    <div className="min-h-screen flex flex-col">
      <header className="border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60 sticky top-0 z-50">
        <div className="container mx-auto px-4 h-16 flex items-center justify-between">
          <Link to="/" className="flex items-center gap-2">
            <Database className="size-8 text-primary" />
            <div className="flex flex-col">
              <span className="font-bold text-lg leading-none">DriftBench</span>
              <span className="text-xs text-muted-foreground">Drift-Aware Database Benchmarking</span>
            </div>
          </Link>

          <nav className="hidden md:flex items-center gap-2">
            <NavLinks />
          </nav>

          <Sheet open={open} onOpenChange={setOpen}>
            <SheetTrigger asChild className="md:hidden">
              <Button variant="outline" size="icon">
                <Menu className="size-5" />
              </Button>
            </SheetTrigger>
            <SheetContent side="right">
              <nav className="flex flex-col gap-2 mt-8">
                <NavLinks />
              </nav>
            </SheetContent>
          </Sheet>
        </div>
      </header>

      <main className="flex-1">
        <Outlet />
      </main>

      <footer className="border-t py-6 bg-muted/50">
        <div className="container mx-auto px-4 text-center text-sm text-muted-foreground">
          <p>
            Based on the research paper by Guanli Liu and Renata Borovica-Gajic, The University of Melbourne
          </p>
          <p className="mt-1">
            <a
              href="https://github.com/Liuguanli/DriftBench"
              target="_blank"
              rel="noopener noreferrer"
              className="text-primary hover:underline"
            >
              View on GitHub
            </a>
          </p>
        </div>
      </footer>
    </div>
  );
}

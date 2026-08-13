import { Link } from "@tanstack/react-router";
import { navItems } from "@/lib/vd-data";

export function TopNav() {
  return (
    <header className="sticky top-0 z-30 bg-navbar/95 backdrop-blur-[2px]">
      <div className="mx-auto flex h-[58px] max-w-[1680px] items-center gap-8 px-8">
        <Link to="/" className="flex items-center gap-2.5 pr-2">
          <Mark />
          <span className="whitespace-nowrap text-[14.5px] font-medium tracking-[-0.02em] text-ink">
            VD Control Center
          </span>
        </Link>

        <nav className="flex items-end gap-0.5">
          {navItems.map((item) => (
            <Link
              key={item.to}
              to={item.to}
              activeOptions={{ exact: item.to === "/" }}
              className="group relative whitespace-nowrap rounded-xs px-2.5 py-1.5 text-[13.5px] text-ink-3 transition-colors duration-150 hover:bg-hover/60 hover:text-ink-2 data-[status=active]:text-ink"
            >
              {item.label}
              <span className="absolute inset-x-2.5 -bottom-[9px] h-px bg-transparent group-data-[status=active]:bg-accent" />
            </Link>
          ))}
        </nav>

        <div className="ml-auto flex items-center gap-4">
          <span className="mono whitespace-nowrap text-[11px] tracking-[0.06em] text-ink-4">
            SIMULATED DATA
          </span>
          <span className="h-4 w-px bg-line" />
          <span className="whitespace-nowrap text-[13px] text-ink-3">prod-conservative</span>
        </div>
      </div>
      <div className="h-px w-full bg-line-strong" />
    </header>
  );
}

function Mark() {
  return (
    <svg width="18" height="18" viewBox="0 0 18 18" aria-hidden="true">
      <path
        d="M2.5 3.5 L9 14.5 L15.5 3.5"
        fill="none"
        stroke="currentColor"
        className="text-accent"
        strokeWidth="1.4"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <circle cx="9" cy="9" r="1.35" className="fill-ink-3" />
    </svg>
  );
}

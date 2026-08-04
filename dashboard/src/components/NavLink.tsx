import type { AnchorHTMLAttributes, ReactNode } from "react";
import { useRouter } from "@/app/router";
import { cn } from "@/lib/utils";

interface NavLinkProps extends Omit<AnchorHTMLAttributes<HTMLAnchorElement>, "href"> {
  to: string;
  exact?: boolean;
  activeClassName?: string;
  children: ReactNode;
}

export function NavLink({
  to,
  exact = false,
  className,
  activeClassName,
  children,
  ...rest
}: NavLinkProps) {
  const { path, navigate } = useRouter();
  const active = exact ? path === to : path === to || path.startsWith(`${to}/`);

  return (
    <a
      href={to}
      aria-current={active ? "page" : undefined}
      className={cn(className, active && activeClassName)}
      onClick={(event) => {
        if (event.metaKey || event.ctrlKey || event.shiftKey || event.button !== 0) return;
        event.preventDefault();
        navigate(to);
      }}
      {...rest}
    >
      {children}
    </a>
  );
}

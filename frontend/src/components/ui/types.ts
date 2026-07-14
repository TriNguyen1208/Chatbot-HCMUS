import { LucideIcon } from "lucide-react";
export interface TabButtonInterface {
  index: number,
  icon: LucideIcon,
  label: string,
  href: string,
}

export interface TabButtonComponent {
  icon: LucideIcon,
  label: string,
  href: string,
  isEntering: boolean,
}
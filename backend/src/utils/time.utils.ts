function parseDurationMs(duration: string): number {
  const units: Record<string, number> = {
    s: 1000,
    m: 60 * 1000,
    h: 60 * 60 * 1000,
    d: 24 * 60 * 60 * 1000,
  };
  
  const match = String(duration).match(/^(\d+)([smhd])$/);
  if (!match) return 7 * 24 * 60 * 60 * 1000;

  const value = parseInt(match[1]!, 10);
  const unit = match[2]!;

  const ms = units[unit]; 
  
  return ms ? value * ms : 7 * 24 * 60 * 60 * 1000;
}
export {
    parseDurationMs
}
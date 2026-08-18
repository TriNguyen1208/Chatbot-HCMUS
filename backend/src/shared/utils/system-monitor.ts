// Import Node.js default os library to get hardware information
import os from "os";

// Function to check if the system is overloaded (Returns true/false)
export const checkSystemLoad = async (): Promise<boolean> => {
    // Calculate the percentage of RAM in use
    const totalMem = os.totalmem();
    const freeMem = os.freemem();
    const usedMemRatio = (totalMem - freeMem) / totalMem;

    // Calculate the percentage of CPU in use
    const loadAvg = os.loadavg();
    const cpuLoadRatio = (loadAvg[0] ?? 1) / (os.cpus().length || 1);

    // Overload threshold: If RAM is used more than 80% OR CPU is more than 80% capacity
    const RAM_THRESHOLD = 0.8;
    const CPU_THRESHOLD = 0.8;

    return (usedMemRatio >= RAM_THRESHOLD && cpuLoadRatio >= CPU_THRESHOLD);
};
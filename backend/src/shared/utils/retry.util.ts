export interface RetryOptions {
    maxRetries?: number;
    baseDelayMs?: number;
    maxDelayMs?: number;
    shouldRetry?: (error: any) => boolean;
    onRetry?: (attempt: number, error: any, nextDelayMs: number) => void;
}


export function calculateJitterDelay(attempt: number, baseDelayMs: number, maxDelayMs: number): number {
    const exponentialLimit = Math.min(maxDelayMs, baseDelayMs * Math.pow(2, attempt));
    return Math.floor(Math.random() * exponentialLimit);
}

export async function retryWithBackoff<T>(
    fn: () => Promise<T>,
    options: RetryOptions = {}
): Promise<T> {
    const {
        maxRetries = 3,
        baseDelayMs = 1000,
        maxDelayMs = 10000,
        shouldRetry,
        onRetry
    } = options;

    let attempt = 0;

    while (true) {
        try {
            return await fn();
        } catch (error: any) {
            attempt++;

            // If maximum retry attempts reached or error is not retryable
            if (attempt > maxRetries || (shouldRetry && !shouldRetry(error))) {
                throw error;
            }

            const delay = calculateJitterDelay(attempt, baseDelayMs, maxDelayMs);

            if (onRetry) {
                onRetry(attempt, error, delay);
            }

            await new Promise((resolve) => setTimeout(resolve, delay));
        }
    }
}

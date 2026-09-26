export interface RetryOptions {
    maxRetries?: number;
    baseDelayMs?: number;
    maxDelayMs?: number;
    shouldRetry?: (error: unknown) => boolean;
    onRetry?: (attempt: number, error: unknown, nextDelayMs: number) => void;
}

/**
 * Calculates Full Jitter sleep duration according to AWS Architecture Best Practice:
 * Sleep = random_between(0, min(maxDelay, baseDelay * 2^attempt))
 */
export function calculateJitterDelay(
    attempt: number,
    baseDelayMs: number,
    maxDelayMs: number,
): number {
    const exponentialLimit = Math.min(
        maxDelayMs,
        baseDelayMs * Math.pow(2, attempt),
    );
    return Math.floor(Math.random() * exponentialLimit);
}

/**
 * Executes an asynchronous function with Exponential Backoff and Full Jitter retry mechanism.
 * 
 * @param fn The asynchronous function to execute
 * @param options Configuration options for retries, backoff delays, and error filters
 */
export async function retryWithBackoff<T>(
    fn: () => Promise<T>,
    options: RetryOptions = {},
): Promise<T> {
    const {
        maxRetries = 3,
        baseDelayMs = 1000,
        maxDelayMs = 10000,
        shouldRetry,
        onRetry,
    } = options;

    let attempt = 0;

    while (true) {
        try {
            return await fn();
        } catch (error: unknown) {
            attempt++;

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

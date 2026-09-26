export enum CircuitState {
    CLOSED = "CLOSED",
    OPEN = "OPEN",
    HALF_OPEN = "HALF_OPEN",
}

export interface CircuitBreakerOptions {
    name?: string;
    failureThresholdRatio?: number; // e.g. 0.5 (50%)
    minimumRequests?: number;       // Minimum samples in window before calculating ratio (e.g. 5)
    samplingPeriodMs?: number;      // Rolling window duration in ms (e.g. 10000ms = 10s)
    cooldownPeriodMs?: number;      // How long circuit stays OPEN before probing in HALF_OPEN (e.g. 30000ms = 30s)
}

interface RequestRecord {
    timestamp: number;
    success: boolean;
}

export class CircuitBreakerOpenException extends Error {
    constructor(circuitName: string, cooldownRemainingMs: number) {
        super(
            `Circuit breaker [${circuitName}] is OPEN. Requests are fast-failing to protect downstream services. Cooldown remaining: ${Math.ceil(cooldownRemainingMs / 1000)}s`
        );
        this.name = "CircuitBreakerOpenException";
    }
}

export class CircuitBreaker {
    private state: CircuitState = CircuitState.CLOSED;
    private records: RequestRecord[] = [];
    private lastStateChangeTime: number = Date.now();

    private readonly name: string;
    private readonly failureThresholdRatio: number;
    private readonly minimumRequests: number;
    private readonly samplingPeriodMs: number;
    private readonly cooldownPeriodMs: number;

    constructor(options: CircuitBreakerOptions = {}) {
        this.name = options.name || "DefaultCircuit";
        this.failureThresholdRatio = options.failureThresholdRatio ?? 0.5;
        this.minimumRequests = options.minimumRequests ?? 5;
        this.samplingPeriodMs = options.samplingPeriodMs ?? 10000;
        this.cooldownPeriodMs = options.cooldownPeriodMs ?? 30000;
    }

    public getState(): CircuitState {
        this.evaluateState();
        return this.state;
    }

    private evaluateState(): void {
        if (this.state === CircuitState.OPEN) {
            const timeSinceOpen = Date.now() - this.lastStateChangeTime;
            if (timeSinceOpen >= this.cooldownPeriodMs) {
                this.state = CircuitState.HALF_OPEN;
                this.lastStateChangeTime = Date.now();
                console.log(`[CircuitBreaker:${this.name}] 🟡 Transitioned from OPEN -> HALF_OPEN (Probing downstream)`);
            }
        }
    }

    private cleanOldRecords(): void {
        const threshold = Date.now() - this.samplingPeriodMs;
        this.records = this.records.filter((r) => r.timestamp >= threshold);
    }

    private recordResult(success: boolean): void {
        this.cleanOldRecords();
        this.records.push({ timestamp: Date.now(), success });

        if (this.state === CircuitState.HALF_OPEN) {
            if (success) {
                this.state = CircuitState.CLOSED;
                this.lastStateChangeTime = Date.now();
                this.records = [];
                console.log(`[CircuitBreaker:${this.name}] 🟢 Downstream healthy. Transitioned from HALF_OPEN -> CLOSED`);
            } else {
                this.state = CircuitState.OPEN;
                this.lastStateChangeTime = Date.now();
                console.warn(`[CircuitBreaker:${this.name}] 🔴 Downstream probe failed. Transitioned from HALF_OPEN -> OPEN`);
            }
            return;
        }

        if (this.state === CircuitState.CLOSED) {
            if (this.records.length >= this.minimumRequests) {
                const failures = this.records.filter((r) => !r.success).length;
                const failureRatio = failures / this.records.length;

                if (failureRatio >= this.failureThresholdRatio) {
                    this.state = CircuitState.OPEN;
                    this.lastStateChangeTime = Date.now();
                    console.error(
                        `[CircuitBreaker:${this.name}] 🔴 Failure ratio reached ${(failureRatio * 100).toFixed(1)}% ` +
                        `(${failures}/${this.records.length} requests failed in ${this.samplingPeriodMs}ms). ` +
                        `Circuit TRIP -> OPEN for ${this.cooldownPeriodMs / 1000}s`
                    );
                }
            }
        }
    }

    /**
     * Executes an operation protected by the Circuit Breaker.
     * Fast-fails immediately if circuit is OPEN.
     */
    async execute<T>(action: () => Promise<T>, fallback?: () => Promise<T>): Promise<T> {
        this.evaluateState();

        if (this.state === CircuitState.OPEN) {
            const cooldownRemaining = Math.max(0, this.cooldownPeriodMs - (Date.now() - this.lastStateChangeTime));
            if (fallback) {
                console.warn(`[CircuitBreaker:${this.name}] Circuit is OPEN, invoking fallback`);
                return await fallback();
            }
            throw new CircuitBreakerOpenException(this.name, cooldownRemaining);
        }

        try {
            const result = await action();
            this.recordResult(true);
            return result;
        } catch (error) {
            this.recordResult(false);
            throw error;
        }
    }
}

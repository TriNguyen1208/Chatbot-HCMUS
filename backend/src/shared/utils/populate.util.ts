import type { IDatabase } from "#@/infrastructure/database/database.interface.js";

/**
 * Cấu hình cho việc populate field
 */
export interface PopulateFieldConfig<T> {
    extractIds: (item: T) => string | string[] | undefined;
    setPopulatedData: (itemResult: any, dataMap: Map<string, any>) => void;
}

/**
 * Hàm dùng chung để populate các field từ một external database (VD: từ MongoDB sang Supabase)
 * @param db Instance của IDatabase (ví dụ: supabaseDB)
 * @param items Danh sách các record gốc cần populate
 * @param tableName Tên bảng ở external DB
 * @param fields Cấu hình các field cần populate (bao gồm cách trích xuất ID và cách gán data)
 * @param ignoreIds Các ID sẽ bỏ qua không query (VD: 'system')
 */
export async function populateFromExternalDB<T, R>(
    db: IDatabase,
    items: T[],
    tableName: string,
    fields: PopulateFieldConfig<T>[],
    ignoreIds: string[] = ['system']
): Promise<R[]> {
    if (!items.length) return [];

    const idsToFetch = new Set<string>();

    // 1. Thu thập tất cả ID cần fetch
    items.forEach(item => {
        fields.forEach(field => {
            const ids = field.extractIds(item);
            if (Array.isArray(ids)) {
                ids.forEach(id => {
                    if (id && !ignoreIds.includes(id)) idsToFetch.add(id);
                });
            } else if (ids && !ignoreIds.includes(ids)) {
                idsToFetch.add(ids);
            }
        });
    });

    // 2. Fetch data từ external DB (query 1 lần duy nhất để tối ưu)
    const dataMap = new Map<string, any>();
    if (idsToFetch.size > 0) {
        const fetchedData = await db.findIn<any>(tableName, 'id', Array.from(idsToFetch));
        fetchedData.forEach(d => {
            if (d.id) dataMap.set(d.id, d);
        });
    }

    // 3. Clone và map data vào các object
    return items.map(item => {
        // Clone shallow để không mutate object gốc
        const result = { ...item } as any;
        
        // Chạy từng setter để map data
        fields.forEach(field => {
            field.setPopulatedData(result, dataMap);
        });

        return result as R;
    });
}

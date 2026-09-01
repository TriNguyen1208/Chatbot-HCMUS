import { formatDistanceToNow } from 'date-fns';
import { vi } from 'date-fns/locale';

export const getRelativeTime = (date: Date | string | undefined): string => {
    if (!date) return '';
    try {
        const parsedDate = new Date(date);
        return formatDistanceToNow(parsedDate, { addSuffix: true, locale: vi });
    } catch (error) {
        return '';
    }
};

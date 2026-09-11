declare module '@microlink/react' {
    import React from 'react';

    export interface MicrolinkProps {
        url: string;
        size?: 'small' | 'normal' | 'large';
        media?: string | string[];
        setData?: (data: any) => any;
        className?: string;
        style?: React.CSSProperties;
        lazy?: boolean | object;
        direction?: 'ltr' | 'rtl';
        contrast?: boolean;
        [key: string]: any;
    }

    const Microlink: React.FC<MicrolinkProps>;
    export default Microlink;
}

declare module 'linkify-react' {
    import React from 'react';

    export interface LinkifyProps {
        options?: any;
        as?: any;
        tagName?: any;
        children?: React.ReactNode;
        [key: string]: any;
    }

    const Linkify: React.FC<LinkifyProps>;
    export default Linkify;
}

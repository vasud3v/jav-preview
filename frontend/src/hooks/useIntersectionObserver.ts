import { useEffect, useRef, useState } from 'react';

interface UseIntersectionObserverProps {
    threshold?: number;
    root?: Element | null;
    rootMargin?: string;
    freezeOnceVisible?: boolean;
}

export function useIntersectionObserver({
    threshold = 0,
    root = null,
    rootMargin = '0%',
    freezeOnceVisible = false,
}: UseIntersectionObserverProps = {}): [React.RefObject<HTMLDivElement | null>, boolean] {
    const [entry, setEntry] = useState<IntersectionObserverEntry>();
    const [frozen, setFrozen] = useState(false);
    const elementRef = useRef<HTMLDivElement>(null);

    const updateEntry = ([entry]: IntersectionObserverEntry[]) => {
        setEntry(entry);
        if (entry.isIntersecting && freezeOnceVisible) {
            setFrozen(true);
        }
    };

    useEffect(() => {
        const node = elementRef.current; // access current value inside effect

        // Safety check for browser support and node existence
        if (typeof window === 'undefined' || !window.IntersectionObserver || frozen || !node) {
            return;
        }

        const observerParams = { threshold, root, rootMargin };
        const observer = new IntersectionObserver(updateEntry, observerParams);

        observer.observe(node);

        return () => observer.disconnect();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [elementRef, threshold, root, rootMargin, frozen]);

    return [elementRef, !!entry?.isIntersecting || frozen];
}

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

        // Always cleanup observer, even if frozen
        return () => {
            observer.disconnect();
        };
    }, [elementRef, JSON.stringify(threshold), root, rootMargin, frozen]);

    return [elementRef, !!entry?.isIntersecting || frozen];
}

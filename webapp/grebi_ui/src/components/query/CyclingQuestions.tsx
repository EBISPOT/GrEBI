import React, { useState, useEffect, useRef, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { get } from "../../app/api";
import { QueryTemplate } from "../../model/QueryTemplate";
import QueryQuestion, { preloadExampleDisplayValues } from "./QueryQuestion";

interface CyclingQuestionsProps {
  graph: string;
  autoPlay?: boolean;
  onVisibleSourceIdsChange?: (currentSourceId: string | null, nextSourceId: string | null) => void;
}

function getExampleSourceId(template: QueryTemplate | null | undefined, exampleIndex: number): string | null {
  if (!template || !template.params?.length) return null;
  const example = template.examples?.[exampleIndex];
  if (!example) return null;
  return example.params[template.params[0].param_id] ?? null;
}

export default function CyclingQuestions({ graph, autoPlay = true, onVisibleSourceIdsChange }: CyclingQuestionsProps) {
  const navigate = useNavigate();
  const [templates, setTemplates] = useState<QueryTemplate[] | null>(null);
  const [currentIndex, setCurrentIndex] = useState(0);
  const [exampleIndices, setExampleIndices] = useState<number[]>([]);
  const [isCycling, setIsCycling] = useState(true);
  const [isHovered, setIsHovered] = useState(false);
  // animKey increments on every transition so CSS animation replays
  const [animKey, setAnimKey] = useState(0);
  const intervalRef = useRef<number | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const transitionInFlightRef = useRef(false);

  useEffect(() => {
    let cancelled = false;

    get<QueryTemplate[]>(`api/v1/graphs/${graph}/query_templates`).then(async (r) => {
      const withQuestions = r.filter((t) => t.question && t.question.trim() !== "");
      const initialExampleIndices = new Array(withQuestions.length).fill(0);

      if (withQuestions.length > 0) {
        await preloadExampleDisplayValues(graph, withQuestions[0], 0);
      }

      if (withQuestions.length > 1) {
        void preloadExampleDisplayValues(graph, withQuestions[1], 0);
      }

      if (cancelled) {
        return;
      }

      setTemplates(withQuestions);
      setExampleIndices(initialExampleIndices);
      setCurrentIndex(0);
    });

    return () => {
      cancelled = true;
    };
  }, [graph]);

  const transitionTo = useCallback(async (nextIndex: number, nextExampleIndices: number[]) => {
    if (!templates || templates.length === 0 || transitionInFlightRef.current) return;

    transitionInFlightRef.current = true;
    try {
      const nextTemplate = templates[nextIndex];
      const nextExampleIndex = nextExampleIndices[nextIndex] ?? 0;
      await preloadExampleDisplayValues(graph, nextTemplate, nextExampleIndex);
      setExampleIndices(nextExampleIndices);
      setCurrentIndex(nextIndex);
      setAnimKey((k) => k + 1);
    } finally {
      transitionInFlightRef.current = false;
    }
  }, [graph, templates]);

  const cycleToNext = useCallback(() => {
    if (!templates || templates.length === 0) return;

    const nextExampleIndices = [...exampleIndices];
    const currentTemplate = templates[currentIndex];
    if (currentTemplate.examples && currentTemplate.examples.length > 0) {
      nextExampleIndices[currentIndex] = (nextExampleIndices[currentIndex] + 1) % currentTemplate.examples.length;
    }

    const nextIndex = (currentIndex + 1) % templates.length;
    void transitionTo(nextIndex, nextExampleIndices);
  }, [currentIndex, exampleIndices, templates, transitionTo]);

  const cycleToPrev = useCallback(() => {
    if (!templates || templates.length === 0) return;
    const nextIndex = (currentIndex - 1 + templates.length) % templates.length;
    void transitionTo(nextIndex, [...exampleIndices]);
  }, [currentIndex, exampleIndices, templates, transitionTo]);

  const goToIndex = useCallback((i: number) => {
    void transitionTo(i, [...exampleIndices]);
  }, [exampleIndices, transitionTo]);

  // Stop/resume cycling when autoPlay changes
  useEffect(() => {
    if (!autoPlay) {
      setIsCycling(false);
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
        intervalRef.current = null;
      }
    } else {
      setIsCycling(true);
    }
  }, [autoPlay]);

  // Auto-cycling
  useEffect(() => {
    if (isCycling && templates && templates.length > 1) {
      intervalRef.current = window.setInterval(cycleToNext, 6000);
    }
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, [isCycling, templates, cycleToNext]);

  // Stop cycling on interaction
  const stopCycling = useCallback(() => {
    setIsCycling(false);
    if (intervalRef.current) {
      clearInterval(intervalRef.current);
      intervalRef.current = null;
    }
  }, []);

  // Keyboard navigation
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (!containerRef.current?.contains(document.activeElement) &&
          !containerRef.current?.matches(":hover")) return;

      if (e.key === "ArrowRight" || e.key === "ArrowDown") {
        e.preventDefault();
        stopCycling();
        cycleToNext();
      } else if (e.key === "ArrowLeft" || e.key === "ArrowUp") {
        e.preventDefault();
        stopCycling();
        cycleToPrev();
      }
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [cycleToNext, cycleToPrev, stopCycling]);

  useEffect(() => {
    if (!templates || templates.length === 0) return;

    const currentExampleIndex = exampleIndices[currentIndex] ?? 0;
    void preloadExampleDisplayValues(graph, templates[currentIndex], currentExampleIndex);

    if (templates.length > 1) {
      const nextIndex = (currentIndex + 1) % templates.length;
      const nextExampleIndex = exampleIndices[nextIndex] ?? 0;
      void preloadExampleDisplayValues(graph, templates[nextIndex], nextExampleIndex);
    }
  }, [currentIndex, exampleIndices, graph, templates]);

  // Notify parent of the current source ID and the next visible one so the
  // homepage can keep a tiny warm cache instead of preloading everything.
  useEffect(() => {
    if (!onVisibleSourceIdsChange || !templates || templates.length === 0) return;

    const currentTemplate = templates[currentIndex];
    const currentExampleIndex = exampleIndices[currentIndex] ?? 0;
    const currentSourceId = getExampleSourceId(currentTemplate, currentExampleIndex);

    let nextSourceId: string | null = null;
    if (templates.length > 1) {
      const nextIndex = (currentIndex + 1) % templates.length;
      const nextTemplate = templates[nextIndex];
      const nextExampleIndex = exampleIndices[nextIndex] ?? 0;
      nextSourceId = getExampleSourceId(nextTemplate, nextExampleIndex);
    }

    onVisibleSourceIdsChange(currentSourceId, nextSourceId);
  }, [currentIndex, exampleIndices, templates, onVisibleSourceIdsChange]);

  if (!templates || templates.length === 0) return null;

  const template = templates[currentIndex];
  const exIdx = exampleIndices[currentIndex] ?? 0;

  return (
    <div
      ref={containerRef}
      className="relative flex items-center cursor-pointer text-black hover:text-blue-600 transition-colors"
      style={{ minHeight: "80px" }}
      onClick={() => {
        stopCycling();
        const example = template.examples?.[exIdx];
        if (example) {
          const qs = new URLSearchParams(example.params).toString();
          navigate(`/graphs/${graph}/queries/${template.id}?${qs}`);
        } else {
          navigate(`/graphs/${graph}/queries/${template.id}`);
        }
      }}
      onFocus={stopCycling}
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
    >
      {/* Left arrow */}
      <button
        className="flex-shrink-0 text-gray-300 hover:text-blue-600 transition-colors p-2"
        onClick={(e) => { e.stopPropagation(); stopCycling(); cycleToPrev(); }}
        title="Previous question"
      >
        <svg width="24" height="24" viewBox="0 0 24 24" fill="currentColor"><path d="M15.41 7.41L14 6l-6 6 6 6 1.41-1.41L10.83 12z"/></svg>
      </button>

      {/* Question with inline editing */}
      <div className="flex-grow flex flex-col items-center">
        <div
          key={animKey}
          className="text-center w-full"
          style={{ animation: "fadeIn 0.35s ease" }}
        >
          <svg className="inline-block mr-2" style={{ verticalAlign: "-0.15em", width: "1.8rem", height: "1.8rem" }} viewBox="0 0 24 24" fill="currentColor"><path d="M15.5 14h-.79l-.28-.27A6.471 6.471 0 0 0 16 9.5 6.5 6.5 0 1 0 9.5 16c1.61 0 3.09-.59 4.23-1.57l.27.28v.79l5 4.99L20.49 19l-4.99-5zm-6 0C7.01 14 5 11.99 5 9.5S7.01 5 9.5 5 14 7.01 14 9.5 11.99 14 9.5 14z"/></svg>
          <QueryQuestion
            key={`${template.id}-${exIdx}`}
            graph={graph}
            template={template}
            exampleIndex={exIdx}
            fontSize="1.8rem"
            readOnly={true}
          />

        </div>

        {/* Dots */}
        <div className="flex justify-center gap-1.5 mt-3">
          {templates.map((_, i) => (
            <button
              key={i}
              className={`rounded-full transition-all ${
                i === currentIndex ? "bg-blue-500 w-4 h-2" : "bg-gray-300 w-2 h-2 hover:bg-gray-400"
              }`}
              onClick={(e) => {
                e.stopPropagation();
                stopCycling();
                goToIndex(i);
              }}
            />
          ))}
        </div>
      </div>

      {/* Right arrow */}
      <button
        className="flex-shrink-0 text-gray-300 hover:text-blue-600 transition-colors p-2"
        onClick={(e) => { e.stopPropagation(); stopCycling(); cycleToNext(); }}
        title="Next question"
      >
        <svg width="24" height="24" viewBox="0 0 24 24" fill="currentColor"><path d="M8.59 16.59L10 18l6-6-6-6-1.41 1.41L13.17 12z"/></svg>
      </button>

      {/* Inline keyframe for fade-in */}
      <style>{`
        @keyframes fadeIn {
          from { opacity: 0; transform: translateY(6px); }
          to { opacity: 1; transform: translateY(0); }
        }
      `}</style>
    </div>
  );
}

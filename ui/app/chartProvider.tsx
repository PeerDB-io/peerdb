'use client';
import {
  BarElement,
  CategoryScale,
  Chart as ChartJS,
  Legend,
  LinearScale,
  LineElement,
  PointElement,
  Title,
  Tooltip,
} from 'chart.js';
import { useEffect } from 'react';

export function ChartJSProvider({ children }: { children: React.ReactNode }) {
  useEffect(() => {
    // Register Chart.js components globally
    ChartJS.register(
      // Basic elements
      BarElement,
      LineElement,
      PointElement,

      // Scales
      CategoryScale,
      LinearScale,

      // Plugins
      Title,
      Tooltip,
      Legend
    );

    // Global Chart.js defaults (optional)
    ChartJS.defaults.responsive = true;
    ChartJS.defaults.maintainAspectRatio = false;
    ChartJS.defaults.animation = false;

    // Cleanup function to unregister on unmount (optional)
    return () => {
      ChartJS.unregister(
        BarElement,
        LineElement,
        PointElement,
        CategoryScale,
        LinearScale,
        Title,
        Tooltip,
        Legend
      );
    };
  }, []);

  return <>{children}</>;
}

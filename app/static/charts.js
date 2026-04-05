// Chart.js utility functions for the finance app

const CHART_COLORS = [
    '#10b981', '#6366f1', '#f59e0b', '#ef4444', '#8b5cf6',
    '#06b6d4', '#ec4899', '#14b8a6', '#f97316', '#84cc16',
    '#a855f7', '#22d3ee', '#fb923c', '#4ade80', '#e879f9',
];

function getChartColor(index) {
    return CHART_COLORS[index % CHART_COLORS.length];
}

function formatCurrency(value) {
    return '£' + Math.abs(value).toLocaleString('en-GB', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
    });
}

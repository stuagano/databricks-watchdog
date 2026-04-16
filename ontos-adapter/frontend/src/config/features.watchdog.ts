// In features.ts, add:
//   import watchdogFeatures from './features.watchdog'
//   export const features = [...existingFeatures, ...watchdogFeatures]

export const watchdogFeatures = [
  {
    id: 'watchdog-violations',
    name: 'Violations',
    path: '/governance/watchdog',
    description: 'Active policy violations across all crawled resources',
    icon: 'ShieldAlert',
    group: 'govern' as const,
    maturity: 'beta' as const,
  },
  {
    id: 'watchdog-policies',
    name: 'Watchdog Policies',
    path: '/governance/policies',
    description: 'Author and manage governance policies evaluated by the Watchdog scanner',
    icon: 'FileText',
    group: 'govern' as const,
    maturity: 'beta' as const,
  },
  {
    id: 'watchdog-exceptions',
    name: 'Exceptions',
    path: '/governance/exceptions',
    description: 'Review, approve, and revoke policy exceptions',
    icon: 'ShieldOff',
    group: 'govern' as const,
    maturity: 'beta' as const,
  },
  {
    id: 'watchdog-remediation',
    name: 'Remediation Review',
    path: '/governance/remediation',
    description: 'Review and approve AI-generated remediation proposals',
    icon: 'Wrench',
    group: 'govern' as const,
    maturity: 'beta' as const,
  },
]

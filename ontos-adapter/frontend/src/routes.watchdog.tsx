// In app.tsx, add inside the /governance route children:
//   import watchdogRoutes from './routes.watchdog'
//   children: [...existingGovernanceChildren, ...watchdogRoutes]

import WatchdogDashboard      from '@/views/WatchdogDashboard'
import WatchdogResourceDetail from '@/views/WatchdogResourceDetail'
import WatchdogPolicies       from '@/views/WatchdogPolicies'
import WatchdogExceptions     from '@/views/WatchdogExceptions'
import RemediationDashboard   from '@/views/RemediationDashboard'
import RemediationInbox       from '@/views/RemediationInbox'

const watchdogRoutes = [
  { path: 'watchdog',                       element: <WatchdogDashboard /> },
  { path: 'watchdog/resources/:resourceId', element: <WatchdogResourceDetail /> },
  { path: 'policies',                       element: <WatchdogPolicies /> },
  { path: 'exceptions',                     element: <WatchdogExceptions /> },
  { path: 'remediation',                    element: <RemediationDashboard /> },
  { path: 'remediation/inbox',              element: <RemediationInbox /> },
]

export default watchdogRoutes

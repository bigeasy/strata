### Set Immediate

Replace `process.nextTick` with `setImmediate` everywhere. Rename the
`nextTick` property passed to the constructor to `setImmediate`. Use
`setImmediate` in the tests.

Additionally, upgrade the target version for Travis CI to 0.10.

### Issue by Issue

 * Rename the `nextTick` property to `setImmediate`. #319.
 * Replace `process.nextTick` with `setImmediate` in tests. #318.
 * Use `setImmediate` instead of `nextTick`. #316.
 * Build with 0.10 on Travis CI. #315.

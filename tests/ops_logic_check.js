const OPS = require('../pulse/web/ops.js'), assert = require('assert');
// band edges: the boundary value belongs to the WORSE band (same as the detector: < is good)
assert.equal(OPS.band('fresh', OPS.bandIndex('fresh', 199)).word, 'Fresh');
assert.equal(OPS.band('fresh', OPS.bandIndex('fresh', 200)).word, 'Aging');
assert.equal(OPS.band('speed', OPS.bandIndex('speed', 49.9)).word, 'Smooth');
assert.equal(OPS.band('speed', OPS.bandIndex('speed', 100)).word, 'Slow');
assert.equal(OPS.band('backlog', OPS.bandIndex('backlog', 200)).word, 'Behind');
assert.equal(OPS.band('rate', OPS.bandIndex('rate', 0.39)).word, 'Collapsed');
assert.equal(OPS.band('rate', OPS.bandIndex('rate', 0.7)).word, 'Normal');
// hysteresis: one bad second does not flip; 3 in a row does; recovery needs 5
let h = new OPS.Hold(); [0, 0, 0].forEach(i => h.push(i));
assert.equal(h.push(2), 0); assert.equal(h.push(0), 0);                       // a single spike is ignored and resets the count
assert.equal(h.push(2), 0); assert.equal(h.push(2), 0); assert.equal(h.push(2), 2);   // 3 consecutive -> worse
for (let k = 0; k < 4; k++) assert.equal(h.push(0), 2);                        // 4 good seconds: still held at the worse band
assert.equal(h.push(0), 0);                                                    // the 5th recovers
// flicker between two bands never flips
h = new OPS.Hold(); h.push(0); let flips = 0, prev = 0; for (let k = 0; k < 60; k++) { const v = h.push(k % 2 ? 1 : 0); flips += v !== prev; prev = v; }
assert.equal(flips, 0);
// ema moves slowly; rounding is to the step
const e = new OPS.Ema(0.2); e.push(40); assert.ok(Math.abs(e.push(140) - 60) < 1e-9); assert.equal(OPS.roundTo(43.7, 10), 40);
// headline: worst wins; a real open incident forces at least 'Degraded'; induced is its own state
assert.equal(OPS.headline(['good', 'good'], false, false).word, 'All good');
assert.equal(OPS.headline(['good', 'warning'], false, false).word, 'Slower than usual');
assert.equal(OPS.headline(['good'], true, false).word, 'Degraded');
assert.equal(OPS.headline(['critical'], false, false).word, 'Feed problem');
assert.equal(OPS.headline(['critical'], true, true).word, 'Test in progress');
// rate ratio needs a healthy baseline and enough bins
assert.equal(OPS.rateRatio(Array(10).fill(10)), null);
assert.equal(OPS.rateRatio(Array(60).fill(1)), null);
assert.ok(Math.abs(OPS.rateRatio([...Array(55).fill(10), 4, 4, 4, 4, 4]) - 0.4) < 1e-9);
console.log('ops logic ok');

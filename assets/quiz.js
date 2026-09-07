/* Review Radar course — shared interactive components.
 *
 * 1. Multiple choice:  <div class="quiz" data-answer="1">
 *                        <div class="q">Question?</div>
 *                        <button class="opt">A</button> <button class="opt">B</button> …
 *                        <div class="fb" data-why="one line shown after answering"></div>
 *                      </div>
 *    data-answer is the 0-based index of the correct option. Options are shuffled on load.
 *
 * 2. Ordering:        <div class="order" data-order="Kafka,Spark,Iceberg"> … chips … </div>
 *                     <div class="order-picked"></div><div class="fb"></div>
 *    Chips are rendered from data-order, shuffled; user clicks them in sequence.
 *
 * 3. Recall box:      <div class="recall"><div class="prompt">…</div>
 *                        <div class="hint">click to reveal</div><div class="answer">…</div></div>
 *
 * A running score is written into any element with id="score".
 */
(function () {
  var total = 0, right = 0;
  function score() {
    var el = document.getElementById('score');
    if (el) el.textContent = right + ' / ' + total + ' first-try correct';
  }
  function shuffle(a) {
    for (var i = a.length - 1; i > 0; i--) {
      var j = Math.floor(Math.random() * (i + 1)); var t = a[i]; a[i] = a[j]; a[j] = t;
    }
    return a;
  }

  // multiple choice
  document.querySelectorAll('.quiz[data-answer]').forEach(function (q) {
    total++;
    var opts = Array.prototype.slice.call(q.querySelectorAll('.opt'));
    var correctText = opts[parseInt(q.dataset.answer, 10)].textContent;
    var fb = q.querySelector('.fb');
    var tries = 0, done = false;
    shuffle(opts).forEach(function (o) { q.insertBefore(o, fb); });
    opts.forEach(function (o) {
      o.addEventListener('click', function () {
        if (done) return;
        tries++;
        if (o.textContent === correctText) {
          o.classList.add('right'); done = true;
          if (tries === 1) right++;
          fb.className = 'fb ok';
          fb.textContent = 'Yes. ' + (fb.dataset.why || '');
          score();
        } else {
          o.classList.add('wrong');
          fb.className = 'fb bad';
          fb.textContent = 'Not that one. Try again.';
        }
      });
    });
  });

  // ordering
  document.querySelectorAll('.order[data-order]').forEach(function (w) {
    total++;
    var target = w.dataset.order.split(',');
    var picked = [], wrongOnce = false;
    var out = w.nextElementSibling, fb = out && out.nextElementSibling;
    shuffle(target.slice()).forEach(function (name) {
      var b = document.createElement('button');
      b.className = 'chip'; b.textContent = name;
      b.addEventListener('click', function () {
        if (b.disabled) return;
        var expected = target[picked.length];
        if (name === expected) {
          picked.push(name); b.disabled = true;
          out.textContent = picked.join(' → ');
          if (picked.length === target.length) {
            fb.className = 'fb ok'; fb.textContent = 'Correct order.';
            if (!wrongOnce) right++;
            score();
          } else { fb.className = 'fb'; fb.textContent = ''; }
        } else {
          wrongOnce = true;
          fb.className = 'fb bad';
          fb.textContent = name + ' is not next. What does a review touch right after ' +
            (picked.length ? picked[picked.length - 1] : 'the raw file') + '?';
        }
      });
      w.appendChild(b);
    });
  });

  // recall boxes
  document.querySelectorAll('.recall').forEach(function (r) {
    r.addEventListener('click', function () { r.classList.toggle('open'); });
  });

  score();
})();

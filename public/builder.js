(() => {
  const HOLD_DELAY_MS = 900;
  const AUTO_SCROLL_EDGE = 110;
  const AUTO_SCROLL_STEP = 22;

  function initCardBuilder(options = {}) {
    const container = document.getElementById(options.containerId || 'cards-container');
    const template = document.getElementById(options.templateId || 'card-template');
    const addBtn = document.getElementById(options.addButtonId || 'add-card-bottom');
    if (!container || !template || !addBtn) return;

    const form = options.formId ? document.getElementById(options.formId) : null;
    const banner = options.bannerId ? document.getElementById(options.bannerId) : null;
    let dragState = null;

    const showBanner = (msg) => {
      if (!banner || !msg) return;
      banner.textContent = msg;
      banner.style.display = 'block';
      setTimeout(() => { banner.style.display = 'none'; }, 2500);
    };

    function updateSelectLock() {
      const active = !!dragState || container.querySelector('.builder-card.drag-ready');
      document.body.classList.toggle('no-text-select', !!active);
      if (active && document.activeElement && document.activeElement !== document.body) {
        document.activeElement.blur();
      }
    }

    function closeMenus() {
      container.querySelectorAll('.builder-card.menu-open').forEach(card => card.classList.remove('menu-open'));
      updateSelectLock();
    }

    function updateOrder() {
      const cards = Array.from(container.querySelectorAll('.builder-card'));
      cards.forEach((card, index) => {
        card.dataset.cardIndex = index;
        const number = card.querySelector('.card-number');
        if (number) number.textContent = index + 1;
        card.querySelectorAll('[data-field]').forEach(input => {
          const field = input.getAttribute('data-field');
          input.name = `cards[${index}][${field}]`;
        });
      });
    }

    function setFrozen(card, frozen) {
      card.classList.toggle('drag-ready', frozen);
      card.classList.toggle('menu-open', frozen);
      card.classList.remove('dragging');
      card.querySelectorAll('.builder-fields input').forEach(input => {
        input.readOnly = frozen;
        input.classList.toggle('input-frozen', frozen);
      });
      updateSelectLock();
    }

    function handleAction(card, action) {
      if (action === 'delete') {
        if (container.children.length <= 1) {
          card.querySelectorAll('input[type="text"]').forEach(inp => inp.value = '');
          const hidden = card.querySelector('[data-field="id"]');
          if (hidden) hidden.value = '';
        } else {
          card.remove();
        }
      } else if (action === 'move-up') {
        const prev = card.previousElementSibling;
        if (prev) container.insertBefore(card, prev);
      } else if (action === 'move-down') {
        const next = card.nextElementSibling;
        if (next) container.insertBefore(next, card);
      } else if (action === 'add-below') {
        const newCard = createCard();
        container.insertBefore(newCard, card.nextElementSibling);
      }
      closeMenus();
      updateOrder();
      updateSelectLock();
    }

    function attachActions(card) {
      const dots = card.querySelector('.dots-btn');
      if (dots) {
        dots.addEventListener('click', (e) => {
          e.preventDefault();
          e.stopPropagation();
          const enable = !card.classList.contains('drag-ready');
          if (enable) {
            closeMenus();
          }
          setFrozen(card, enable);
        });
      }
      card.querySelectorAll('[data-action]').forEach(btn => {
        btn.addEventListener('click', (e) => {
          e.preventDefault();
          e.stopPropagation();
          const action = btn.getAttribute('data-action');
          handleAction(card, action);
        });
      });
    }

    function autoScroll(clientY) {
      if (clientY < AUTO_SCROLL_EDGE) {
        window.scrollBy({ top: -AUTO_SCROLL_STEP, behavior: 'auto' });
      } else if (clientY > window.innerHeight - AUTO_SCROLL_EDGE) {
        window.scrollBy({ top: AUTO_SCROLL_STEP, behavior: 'auto' });
      }
    }

    function placePlaceholder(pageY) {
      if (!dragState) return;
      const { card, placeholder } = dragState;
      const siblings = Array.from(container.querySelectorAll('.builder-card')).filter(el => el !== card);
      let placed = false;
      for (const sibling of siblings) {
        const rect = sibling.getBoundingClientRect();
        const middle = rect.top + window.scrollY + rect.height / 2;
        if (pageY < middle) {
          container.insertBefore(placeholder, sibling);
          placed = true;
          break;
        }
      }
      if (!placed) container.appendChild(placeholder);
    }

    function finishDrag() {
      if (!dragState) return;
      const { card, placeholder } = dragState;
      card.classList.remove('dragging');
      card.removeAttribute('style');
      container.insertBefore(card, placeholder);
      placeholder.remove();
      dragState = null;
      updateOrder();
      updateSelectLock();
      card.classList.add('drag-settled');
      setTimeout(() => card.classList.remove('drag-settled'), 300);
    }

    function startDrag(card, startEvt) {
      if (dragState) return;
      const rect = card.getBoundingClientRect();
      const placeholder = document.createElement('div');
      placeholder.className = 'card-placeholder';
      placeholder.style.height = `${rect.height}px`;
      dragState = {
        card,
        placeholder,
        offsetY: startEvt.clientY - rect.top,
        left: rect.left
      };
      container.insertBefore(placeholder, card.nextElementSibling);

      card.classList.add('dragging');
      updateSelectLock();
      card.style.width = `${rect.width}px`;
      card.style.height = `${rect.height}px`;
      card.style.left = `${rect.left}px`;
      card.style.top = `${rect.top}px`;
      card.style.position = 'fixed';
      card.style.pointerEvents = 'none';
      card.style.zIndex = '60';

      const onMove = (e) => {
        e.preventDefault();
        const clientY = e.clientY;
        if (!Number.isFinite(clientY)) return;
        card.style.top = `${clientY - dragState.offsetY}px`;
        card.style.left = `${dragState.left}px`;
        autoScroll(clientY);
        placePlaceholder(clientY + window.scrollY);
      };

      const onUp = () => {
        document.removeEventListener('pointermove', onMove);
        document.removeEventListener('pointerup', onUp);
        finishDrag();
      };

      document.addEventListener('pointermove', onMove);
      document.addEventListener('pointerup', onUp);
    }

    function attachDrag(card) {
      card.addEventListener('pointerdown', (e) => {
        if (!card.classList.contains('drag-ready')) return;
        if (e.button !== 0 && e.pointerType !== 'touch') return;
        if (e.target.closest('.card-actions') || e.target.closest('.dots-btn')) return;

        const startEvt = e;
        let moved = false;

        const detectMove = (moveEvt) => {
          if (Math.abs(moveEvt.clientY - startEvt.clientY) > 6 || Math.abs(moveEvt.clientX - startEvt.clientX) > 6) {
            moved = true;
          }
        };

        const cancel = () => {
          clearTimeout(timer);
          document.removeEventListener('pointermove', detectMove);
          document.removeEventListener('pointerup', cancel);
        };

        const timer = setTimeout(() => {
          if (!moved) startDrag(card, startEvt);
          cancel();
        }, HOLD_DELAY_MS);

        document.addEventListener('pointermove', detectMove);
        document.addEventListener('pointerup', cancel);
      });
    }

    function createCard() {
      const clone = template.content.firstElementChild.cloneNode(true);
      attachActions(clone);
      attachDrag(clone);
      return clone;
    }

    addBtn.addEventListener('click', () => {
      const newCard = createCard();
      container.appendChild(newCard);
      updateOrder();
      newCard.scrollIntoView({ behavior: 'smooth', block: 'center' });
      const heb = newCard.querySelector('[data-field="hebrew"]');
      if (heb) setTimeout(() => heb.focus(), 180);
    });

    container.querySelectorAll('.builder-card').forEach(card => {
      attachActions(card);
      attachDrag(card);
    });

    document.addEventListener('click', (e) => {
      if (!e.target.closest('.builder-card')) closeMenus();
    });
    updateOrder();

    if (form) {
      form.addEventListener('submit', (e) => {
        if (!form.reportValidity()) {
          e.preventDefault();
          const invalid = form.querySelector(':invalid');
          if (invalid) {
            invalid.scrollIntoView({ behavior: 'smooth', block: 'center' });
            invalid.focus({ preventScroll: true });
          }
          showBanner('Complete les champs obligatoires.');
        }
      });
    }
  }

  window.initCardBuilder = initCardBuilder;
})();

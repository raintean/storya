const template = `
  <style>
    :host {
      display: block;
      overflow: hidden;
      color: white;
      background: #050505;
      border-radius: 12px;
      aspect-ratio: 16 / 9;
    }

    video {
      display: block;
      width: 100%;
      height: 100%;
      object-fit: contain;
    }
  </style>
  <video></video>
`

export function defineStoryaPlayer(tagName = 'storya-player'): void {
  if (typeof globalThis.customElements === 'undefined' || customElements.get(tagName)) {
    return
  }

  class StoryaPlayerElement extends HTMLElement {
    static readonly observedAttributes = ['autoplay', 'controls', 'muted', 'poster', 'src']

    readonly #video: HTMLVideoElement

    constructor() {
      super()

      const root = this.attachShadow({ mode: 'open' })
      root.innerHTML = template

      const video = root.querySelector('video')
      if (!(video instanceof HTMLVideoElement)) {
        throw new Error('storya-player failed to initialize its video element')
      }

      this.#video = video
    }

    connectedCallback(): void {
      this.#syncAttributes()
    }

    attributeChangedCallback(): void {
      this.#syncAttributes()
    }

    #syncAttributes(): void {
      this.#video.autoplay = this.hasAttribute('autoplay')
      this.#video.controls = this.hasAttribute('controls')
      this.#video.muted = this.hasAttribute('muted')
      this.#video.playsInline = true
      this.#video.poster = this.getAttribute('poster') ?? ''
      this.#video.src = this.getAttribute('src') ?? ''
    }
  }

  customElements.define(tagName, StoryaPlayerElement)
}

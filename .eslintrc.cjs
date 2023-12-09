module.exports = {
  env: {
    node: true,
    es2021: true
  },
  parserOptions: {
    ecmaVersion: 'latest',
    sourceType: 'module'
  },
  extends: [
    'eslint:recommended',
  ],
  plugins: [
    '@stylistic/js'
  ],
  overrides: [],
  rules: {
    '@stylistic/js/quotes': ['error', 'single'],
    'no-constant-condition': 'off'
  }
}

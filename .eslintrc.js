module.exports = {
  "parser": "@typescript-eslint/parser",
  "plugins": ["@typescript-eslint"],
  "extends": ["airbnb-base"],
  "settings": {
    "import/extensions": [".ts"],
    "import/parsers": {
      "@typescript-eslint/parser": [".ts"]
    },
    "import/resolver": {
      "node": {
        "extensions": [".ts"]
      }
    }
  },
  "env": {
    "jest": true,
    "commonjs": true,
    "es6": true,
    "node": true
  },
  "parserOptions": {
    "ecmaVersion": 2018
  },
  "rules": {
    "@typescript-eslint/no-unused-vars": [
      "error",
      { "argsIgnorePattern": "next" }
    ],
    "no-underscore-dangle": "off",
    "class-methods-use-this": "off",
    "camelcase": "off",
    "import/extensions": "off",
    "no-param-reassign": "off",
    "semi": "off",
    "no-plusplus": "off",
    "no-unused-vars": ["error", { "argsIgnorePattern": "next" }]
  }
}

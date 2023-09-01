interface ParameterInput {
  name?: string;
  type?: string;
  value?: any;
}

export default class DBSQLParameter {
  name?: string;

  type?: string;

  value?: any;

  public constructor({ name, type, value }: ParameterInput) {
    this.name = name;
    this.type = type;
    this.value = value;
  }
}

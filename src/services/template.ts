import Handlebars from 'handlebars';

// Template context interface
export interface TemplateContext {
  contact: {
    email: string;
    name?: string;
    firstName?: string;
    lastName?: string;
    [key: string]: any;
  };
  sender: {
    email: string;
    name?: string;
    firstName?: string;
    lastName?: string;
    [key: string]: any;
  };
  sequence: {
    id: string;
    name: string;
    [key: string]: any;
  };
  step: {
    number: number;
    subject: string;
    [key: string]: any;
  };
  custom?: {
    [key: string]: any;
  };
}

// Register common Handlebars helpers
Handlebars.registerHelper('capitalize', function(str: string) {
  if (!str) return '';
  return str.charAt(0).toUpperCase() + str.slice(1).toLowerCase();
});

Handlebars.registerHelper('upper', function(str: string) {
  return str ? str.toUpperCase() : '';
});

Handlebars.registerHelper('lower', function(str: string) {
  return str ? str.toLowerCase() : '';
});

Handlebars.registerHelper('default', function(value: any, defaultValue: string) {
  return value || defaultValue;
});

Handlebars.registerHelper('eq', function(a: any, b: any) {
  return a === b;
});

Handlebars.registerHelper('ne', function(a: any, b: any) {
  return a !== b;
});

Handlebars.registerHelper('gt', function(a: number, b: number) {
  return a > b;
});

Handlebars.registerHelper('lt', function(a: number, b: number) {
  return a < b;
});

Handlebars.registerHelper('and', function(...args: any[]) {
  // Remove the handlebars options object
  args.pop();
  return args.every(Boolean);
});

Handlebars.registerHelper('or', function(...args: any[]) {
  // Remove the handlebars options object
  args.pop();
  return args.some(Boolean);
});

Handlebars.registerHelper('formatDate', function(date: Date | string, format: string = 'YYYY-MM-DD') {
  if (!date) return '';
  const d = new Date(date);
  if (isNaN(d.getTime())) return '';
  
  // Simple date formatting (could be enhanced with date-fns or moment)
  switch (format) {
    case 'YYYY-MM-DD':
      return d.toISOString().split('T')[0];
    case 'MM/DD/YYYY':
      return `${(d.getMonth() + 1).toString().padStart(2, '0')}/${d.getDate().toString().padStart(2, '0')}/${d.getFullYear()}`;
    case 'long':
      return d.toLocaleDateString('en-US', { 
        year: 'numeric', 
        month: 'long', 
        day: 'numeric' 
      });
    default:
      return d.toISOString();
  }
});

export class TemplateService {
  private compiledTemplates = new Map<string, HandlebarsTemplateDelegate>();

  /**
   * Compile a template string
   */
  compile(template: string): HandlebarsTemplateDelegate {
    return Handlebars.compile(template);
  }

  /**
   * Render a template with context
   */
  render(template: string, context: TemplateContext): string {
    try {
      // Check if we have a compiled version cached
      let compiledTemplate = this.compiledTemplates.get(template);
      
      if (!compiledTemplate) {
        compiledTemplate = this.compile(template);
        this.compiledTemplates.set(template, compiledTemplate);
      }

      return compiledTemplate(context);
    } catch (error) {
      throw new Error(`Template rendering failed: ${error.message}`);
    }
  }

  /**
   * Render subject line
   */
  renderSubject(subject: string, context: TemplateContext): string {
    return this.render(subject, context);
  }

  /**
   * Render email body
   */
  renderBody(body: string, context: TemplateContext): string {
    return this.render(body, context);
  }

  /**
   * Create template context from enrollment data
   */
  createContext(enrollment: any, step: any, mailbox: any): TemplateContext {
    // Extract first and last name from contact name
    const contactParts = enrollment.contactName ? enrollment.contactName.split(' ') : [];
    const contactFirstName = contactParts[0] || '';
    const contactLastName = contactParts.slice(1).join(' ') || '';

    // Extract first and last name from sender name
    const senderParts = mailbox.displayName ? mailbox.displayName.split(' ') : [];
    const senderFirstName = senderParts[0] || '';
    const senderLastName = senderParts.slice(1).join(' ') || '';

    return {
      contact: {
        email: enrollment.contactEmail,
        name: enrollment.contactName,
        firstName: contactFirstName,
        lastName: contactLastName,
      },
      sender: {
        email: mailbox.email,
        name: mailbox.displayName,
        firstName: senderFirstName,
        lastName: senderLastName,
      },
      sequence: {
        id: enrollment.sequence.id,
        name: enrollment.sequence.name,
      },
      step: {
        number: step.stepNumber,
        subject: step.subject,
      },
    };
  }

  /**
   * Validate template syntax
   */
  validate(template: string): { valid: boolean; error?: string } {
    try {
      this.compile(template);
      return { valid: true };
    } catch (error) {
      return { 
        valid: false, 
        error: error.message 
      };
    }
  }

  /**
   * Clear template cache
   */
  clearCache(): void {
    this.compiledTemplates.clear();
  }

  /**
   * Get template variables from a template string
   */
  extractVariables(template: string): string[] {
    const variables: string[] = [];
    const regex = /\{\{\s*([^}]+)\s*\}\}/g;
    let match;

    while ((match = regex.exec(template)) !== null) {
      const variable = match[1].trim();
      // Remove handlebars helpers and extract just the variable name
      const cleanVariable = variable.split(' ')[0].replace(/^[#^\/]/, '');
      if (!variables.includes(cleanVariable)) {
        variables.push(cleanVariable);
      }
    }

    return variables;
  }
}

export const templateService = new TemplateService();